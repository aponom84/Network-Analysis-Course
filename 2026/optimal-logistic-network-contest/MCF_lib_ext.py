import os
import pandas as pd
from collections import defaultdict
import json
import math
import logging


class EmojiHandler(logging.StreamHandler):
    """Кастомный обработчик с эмодзи вместо уровней логирования"""
    
    def emit(self, record):
        # Заменяем уровни логирования на эмодзи
        level_to_emoji = {
            'DEBUG': '🐛',
            'INFO': 'ℹ️',
            'WARNING': '⚠️',
            'ERROR': '❌',
            'CRITICAL': '💥'
        }
        
        # Сохраняем оригинальный уровень
        original_levelname = record.levelname
        
        # Заменяем уровень на эмодзи
        record.levelname = level_to_emoji.get(record.levelname, '❓')        
        # Вызываем родительский метод для форматирования и вывода
        super().emit(record)
        
        # Восстанавливаем оригинальный уровень
        record.levelname = original_levelname

def setup_logger(name=__name__):
    """Настройка и возврат кастомного логгера"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Очищаем все существующие обработчики
    logger.handlers.clear()
    
    # Добавляем наш кастомный обработчик
    handler = EmojiHandler()
    formatter = logging.Formatter('%(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Отключаем распространение логов к корневому логгеру
    logger.propagate = False
    
    return logger

# Создаем глобальный логгер по умолчанию
logger = setup_logger()

def format_time(seconds):
    days = seconds // (24 * 3600)
    seconds %= (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    
    parts = []
    if days > 0:
        parts.append(f"{int(days)} дн")
    if hours > 0:
        parts.append(f"{int(hours)} ч")
    if minutes > 0 or not parts:  # показываем минуты даже если 0, если нет других единиц
        parts.append(f"{int(minutes)} мин")
    return " ".join(parts)

class Solution:
    """Класс для хранения и анализа решения задачи Multi Commodity Flow.

    Сущности: 
    paths: List[Dist[(Int, Int)]->Dict] 

    paths: Список путей (каждый путь - словарь с ключами 'src', 'dst', 'volume', 'path_nodes')
            vehicle_capacity: Вместимость ТС (м³)
    
    transport_legs: Dict([src, dst])- информация о транспортных плечах (о рёбрах)
      key: (src, dist) – начало плеча и конец плеча
      Содержимое каждой записи это тоже словарь с полями:
        vehicles (число транспортных средств)
        sum_cost – общая стоимость перевозки груза
        cost_per_km – тариф дял данного (возможно лишнее поле т.к. можно взять из matrix_df)
        distance – расстояние (тоже лишняя инфа)
        time - требуемое время для одного транспортного средства проехать из src в dst
        sum_volume: суммарный обслуживаемй объём груза по заявкам
        reqs: List(Tuple(Int, Int)): список обсуживаемых заявок в виде пар ((src, dst), volume)
    """
    
    def __init__(self, paths, vehicle_capacity=90.0, matrix_df=None, offices_df=None):
        """
        Инициализация решения. Достаточно передать либо paths, либо transport_legs.
        
        Args:
            paths: Список путей (каждый путь - словарь с ключами 'src', 'dst', 'volume', 'path_nodes')
            vehicle_capacity: Вместимость ТС (м³)
            matrix_df: DataFrame с матрицей расстояния, времени, и тарифом стоимостью проезда за километр:
            offices_df: DataFrame с информацией об офисах (перегрузных узлах)
        """

        self.paths = paths
        self.vehicle_capacity = vehicle_capacity
        self.matrix_df = matrix_df
        self.offices_df = offices_df
        self.transport_legs = self._reconstruct_transport_legs_from_paths()
        self.reqs = self._reconstruct_reqs_from_paths()
        
        # Вычисляем все метрики
        self._compute_metrics()
    
    def _reconstruct_reqs_from_paths(self):
        """Востанавливает заявки по путям"""
        reqs = defaultdict(int)
        for path in self.paths:
            volume = path['volume']
            src = path['src']
            dst = path['dst']
            reqs[(src, dst)] = reqs.get( (src, dst), 0) + volume

        return reqs
    
    def _reconstruct_transport_legs_from_paths(self):
        """Восстанавливает transport_legs из paths."""
        transport_legs = {}

        for path in self.paths:
            path_nodes = path['path_nodes']
            volume = path['volume']
            src = path['src']
            dst = path['dst']

            edges = [(path_nodes[i], path_nodes[i+1]) for i in range(len(path_nodes)-1)]
            
            for (s, t) in edges:
                key = (s, t)
                if key not in transport_legs:
                    try:
                        transport_legs[key] = {
                            'distance': self.matrix_df.loc[s, t]['distance'],
                            'time': self.matrix_df.loc[s, t]['time'],
                            'cost_per_km': self.matrix_df.loc[s, t]['price_per_km'],
                            'sum_volume': 0,
                            'reqs': []
                        }
                    except KeyError:
                        logger.critical(f'Нет данных в matrix_df для ребра {s} → {t}')
                        continue  # пропускаем проблемное ребро

                transport_legs[key]['reqs'].append(((src, dst), volume))
                transport_legs[key]['sum_volume'] += volume

        # Вычисляем количество ТС и стоимость
        for leg in transport_legs.values():
            leg['vehicles'] = math.ceil(leg['sum_volume'] / self.vehicle_capacity)
            leg['sum_cost'] = leg['vehicles'] * leg['cost_per_km'] * leg['distance'] / 1000.0

        return transport_legs

    def _calculate_transfer_costs(self):
        """Вычисляет стоимость перегруза для всех транзитных узлов."""
        total_transfer_cost = 0.0
        transfer_violations = []
        
        # Создаем словарь для отслеживания потоков через узлы
        node_in_flow = defaultdict(float)
        node_out_flow = defaultdict(float)
        
        for path in self.paths:
            path_nodes = path['path_nodes']
            volume = path['volume']
            src = path['src']
            dst = path['dst']
            
            # Учитываем транзитные узлы (исключаем источник и сток)
            transit_nodes = path_nodes[1:-1] if len(path_nodes) > 2 else []
            
            for node in transit_nodes:
                # Добавляем стоимость перегруза
                if self.offices_df is not None and node in self.offices_df.index:
                    transfer_price = self.offices_df.loc[node, 'transfer_price']
                    total_transfer_cost += volume * transfer_price
                
                # Учитываем потоки для проверки ограничений
                node_in_flow[node] += volume
                node_out_flow[node] += volume
        
        # Проверяем ограничения пропускной способности узлов
        if self.offices_df is not None:
            for node_id, row in self.offices_df.iterrows():
                transfer_max = row['transfer_max']
                
                if node_id in node_in_flow and node_in_flow[node_id] > transfer_max:
                    transfer_violations.append(
                        f"Узел {node_id}: входящий поток {node_in_flow[node_id]:.2f} > {transfer_max}"
                    )
                
                if node_id in node_out_flow and node_out_flow[node_id] > transfer_max:
                    transfer_violations.append(
                        f"Узел {node_id}: исходящий поток {node_out_flow[node_id]:.2f} > {transfer_max}"
                    )
        
        return total_transfer_cost, transfer_violations
    
    def _compute_metrics(self):
        """Вычисляет все метрики на основе имеющихся данных."""
        # Общий объем груза
        self.total_volume = sum(path['volume'] for path in self.paths)
        
        # Общее количество ТС
        self.total_vehicles = sum(leg['vehicles'] for leg in self.transport_legs.values())
        
        # Общая дистанция (метры)
        self.total_distance = sum(leg['distance'] * leg['vehicles'] for leg in self.transport_legs.values())

        # Общее время транспортировки (секунды)
        self.total_time = sum(leg['time'] * leg['vehicles'] for leg in self.transport_legs.values())
        
        # Базовая стоимость перевозки
        self.transport_cost = sum(leg['sum_cost'] for leg in self.transport_legs.values())
        
        # Стоимость перегруза и проверка ограничений
        self.transfer_cost, self.transfer_violations = self._calculate_transfer_costs()
        
        # Общая стоимость (перевозка + перегруз)
        self.total_cost = self.transport_cost + self.transfer_cost
        
        # Среднее время доставки одной доставки в секундах
        self.avg_delivery_time_per_req = self.total_time / len(self.reqs.keys()) if self.reqs else 0

        # Среднее время доставки одной единицы объёма груза
        self.avg_delivery_time_per_volume = self.total_time / sum(self.reqs.values()) if sum(self.reqs.values()) > 0 else 0

        self.paths_per_req = len(self.paths) / len(self.reqs.keys()) if self.reqs else 0
        
        # Процент загруженности ТС
        total_capacity = self.total_vehicles * self.vehicle_capacity
        self.vehicle_utilization = min(
            (self.total_volume / total_capacity * 100) if total_capacity > 0 else 0.0, 100.0
        )

        self.underloaded_legs = self.count_underloaded_legs()
        
        # Стоимость на кубометр
        self.cost_per_cubic_meter = self.total_cost / self.total_volume if self.total_volume > 0 else float('inf')
    
    def validate_coverage(self, df_tare):
        """Проверяет, что все заявки из df_tare учтены и объёмы совпадают."""
        actual = defaultdict(float)

        for path in self.paths:
            actual[(path['src'], path['dst'])] += path['volume']

        errors = []
        for (src, dst), exp_vol in df_tare['volume'].items():
            act_vol = actual.get((src, dst), 0)
            if abs(act_vol - exp_vol) > 1e-3:
                errors.append(f"Заявка {src}→{dst}: ожидалось {exp_vol}, получено {act_vol:.2f}")

        return errors

    def count_underloaded_legs(self, threshold=0.3):
        """Считает количество рёбер с загрузкой < threshold * vehicle_capacity."""
        count = 0
        for leg in self.transport_legs.values():
            if leg['sum_volume'] < threshold * self.vehicle_capacity:
                count += 1
        return count
    
    def save_to_csv(self, output_dir):
        """Сохраняет решение в CSV файлы в указанной директории."""
        os.makedirs(output_dir, exist_ok=True)
        
        # Сохранение путей
        if self.paths:
            paths_df = pd.DataFrame(self.paths)
            paths_df.to_csv(os.path.join(output_dir, 'paths.csv'), index=False)
        
        # Сохранение транспортных этапов (плеч)
        if self.transport_legs:
            # Преобразуем вложенные структуры для сохранения в CSV
            flat_legs = []
            for leg in self.transport_legs.values():
                flat_leg = leg.copy()
                if 'reqs' in flat_leg and flat_leg['reqs']:
                    # Преобразуем список заявок в строку для CSV
                    flat_leg['reqs_str'] = '; '.join(
                        [f"{src}->{dst}({vol:.2f})" for (src, dst), vol in flat_leg['reqs']]
                    )
                flat_legs.append(flat_leg)
            
            usage_df = pd.DataFrame(flat_legs)
            usage_df.to_csv(os.path.join(output_dir, 'transport_legs.csv'), index=False)
        
        # Сохранение сводки
        summary = pd.DataFrame({
            'total_cost': [self.total_cost],
            'transport_cost': [self.transport_cost],
            'transfer_cost': [self.transfer_cost],
            'total_vehicles': [self.total_vehicles],
            'total_distance': [self.total_distance],
            'total_volume': [self.total_volume],
            'vehicle_utilization': [self.vehicle_utilization],
            'cost_per_cubic_meter': [self.cost_per_cubic_meter],
            'avg_delivery_time': [self.avg_delivery_time_per_req],
            'vehicle_capacity': [self.vehicle_capacity]
        })
        summary.to_csv(os.path.join(output_dir, 'summary.csv'), index=False)
    
    @classmethod
    def load_from_csv(cls, input_file_path, vehicle_capacity=90.0, matrix_df=None, offices_df=None):
        """Загружает решение из CSV файлов из указанной директории."""

        paths = []
        if os.path.exists(input_file_path):
            paths_df = pd.read_csv(input_file_path)
            paths = paths_df.to_dict(orient='records')
        else:
            raise ValueError("Не найдено данных для восстановления решения")
                
        return cls(paths=paths, vehicle_capacity=vehicle_capacity, matrix_df=matrix_df, offices_df=offices_df)
    
    def save_to_json(self, output_dir):
        """Сохраняет решение в JSON файлы."""
        os.makedirs(output_dir, exist_ok=True)
        
        # Подготовка данных для JSON (убираем несериализуемые объекты)
        transport_legs_serializable = []
        for leg in self.transport_legs.values():
            serializable_leg = leg.copy()
            # Преобразуем reqs в сериализуемый формат
            if 'reqs' in serializable_leg:
                serializable_leg['reqs'] = [
                    [(src, dst), vol] for (src, dst), vol in serializable_leg['reqs']
                ]
            transport_legs_serializable.append(serializable_leg)
        
        with open(os.path.join(output_dir, 'solution.json'), 'w') as f:
            json.dump({
                'paths': self.paths,
                'transport_legs': transport_legs_serializable,
                'metadata': {
                    'vehicle_capacity': self.vehicle_capacity,
                    'total_cost': self.total_cost,
                    'transport_cost': self.transport_cost,
                    'transfer_cost': self.transfer_cost,
                    'total_vehicles': self.total_vehicles,
                    'total_distance': self.total_distance,
                    'total_volume': self.total_volume,
                    'vehicle_utilization': self.vehicle_utilization,
                    'cost_per_cubic_meter': self.cost_per_cubic_meter,
                    'avg_delivery_time': self.avg_delivery_time_per_req
                }
            }, f, indent=2, default=str)
    
    @classmethod
    def load_from_json(cls, input_dir, vehicle_capacity=90.0, matrix_df=None, offices_df=None):
        """Загружает решение из JSON файла."""
        json_path = os.path.join(input_dir, 'solution.json')
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Файл решения не найден: {json_path}")
        
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Восстанавливаем структуру reqs
        transport_legs = []
        for leg in data['transport_legs']:
            if 'reqs' in leg:
                leg['reqs'] = [((src, dst), vol) for (src, dst), vol in leg['reqs']]
            transport_legs.append(leg)
        
        # Определяем, что загружать
        if data['paths']:
            return cls(paths=data['paths'], vehicle_capacity=vehicle_capacity, matrix_df=matrix_df, offices_df=offices_df)
        else:
            return cls(paths=None, transport_legs=transport_legs, vehicle_capacity=vehicle_capacity, matrix_df=matrix_df, offices_df=offices_df)
        
    @classmethod
    def load_from_external_format(cls, data, matrix_df, offices_df=None, vehicle_capacity=90.0):
        """
        Загружает решение из внешнего формата (например, JSON с полями src_office_id, dst_office_id, legs).
        
        Args:
            data: dict с ключом 'flows' — список заявок с маршрутами
            matrix_df: DataFrame с матрицей расстояний
            offices_df: DataFrame с информацией об офисах
            vehicle_capacity: вместимость ТС
        
        Returns:
            Экземпляр Solution
        """
        paths = []

        for flow in data.get('flows', []):
            src = flow['src_office_id']
            dst = flow['dst_office_id']
            volume = flow['avg_day_polybox_qty']  # или другой объём, если нужно пересчитать
            legs = flow['legs']

            # Путь узлов: начинаем с src, добавляем все to из legs
            path_nodes = [legs[0]['from_office_id']]
            for leg in legs:
                path_nodes.append(leg['to_office_id'])

            # Проверка: совпадает ли путь с заявкой?
            if path_nodes[0] != src or path_nodes[-1] != dst:
                raise ValueError(f"Путь для заявки {src}->{dst} не начинается или не заканчивается правильно: {path_nodes}")

            paths.append({
                'src': src,
                'dst': dst,
                'volume': float(volume),
                'path_nodes': path_nodes
            })

        # Создаём и возвращаем Solution
        return cls(
            paths=paths,
            vehicle_capacity=vehicle_capacity,
            matrix_df=matrix_df,
            offices_df=offices_df
        )


    def __str__(self):
        """Возвращает краткое описание решения."""
        return (f"Solution - "
                f"Cost: {self.total_cost:.2f}, "
                f"Vehicles: {self.total_vehicles:.1f}, "
                f"Utilization: {self.vehicle_utilization:.1f}%")
    
    
    def detailed_report(self, solution_name=''):
        """Возвращает подробный отчет о решении."""
        report = [
            f"📊 Детальный отчет о решении: {solution_name}",
            f"• Общая стоимость: {self.total_cost:,.2f} руб.",
            f"• Стоимость перевозки: {self.transport_cost:,.2f} руб.",
            f"• Стоимость перегруза: {self.transfer_cost:,.2f} руб.",
            f"• Количество ТС: {self.total_vehicles:.0f}",
            f"• Процент загруженности ТС: {self.vehicle_utilization:.1f}%",
            f"• Количество плеч с загрузкой < 30%: {self.underloaded_legs}",
            f"• Стоимость на кубометр: {self.cost_per_cubic_meter:.2f} руб./м³",
            f"• Общий объем груза: {self.total_volume:,.2f} м³",
            f"• Общая дистанция: {self.total_distance/1000:,.2f} км",
            f"• Среднее время доставки одной заявки: {format_time(self.avg_delivery_time_per_req)}",
            f"• Среднее время доставки одного м³: {format_time(self.avg_delivery_time_per_volume)}",
            f"• Общее количество путей: {len(self.paths)}",
            f"• Количество путей на заявку: {self.paths_per_req:.2f}",
            f"• Вместимость ТС: {self.vehicle_capacity} м³"
        ]
        
        # Добавляем информацию о нарушениях ограничений перегруза
        if self.transfer_violations:
            report.append("❌ Нарушения ограничений перегруза:")
            for violation in self.transfer_violations:
                report.append(f"   {violation}")
        else:
            report.append("✅ Ограничения перегруза соблюдены.")
                
        return "\n".join(report)
