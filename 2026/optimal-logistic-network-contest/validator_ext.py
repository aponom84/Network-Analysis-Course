#!python

import argparse
import json
import os

import pandas as pd
from MCF_lib_ext import Solution, logger


def load_data(input_dir):
    """Загружает reqs.csv, distance_matrix.csv и offices.csv."""
    reqs_path = os.path.join(input_dir, 'reqs.csv')
    matrix_path = os.path.join(input_dir, 'distance_matrix.csv')
    offices_path = os.path.join(input_dir, 'offices.csv')

    if not os.path.exists(reqs_path):
        raise FileNotFoundError(f"Не найден файл: {reqs_path}")
    if not os.path.exists(matrix_path):
        raise FileNotFoundError(f"Не найден файл: {matrix_path}")

    logger.info("✅ Загрузка данных...")
    
    # Загрузка заявок
    df = pd.read_csv(reqs_path)
    df.set_index(['src_office_id', 'dst_office_id'], inplace=True)
    # Преобразуем volume в float
    df['volume'] = df['volume'].astype(float)

    # Загрузка матрицы расстояний
    matrix_df = pd.read_csv(matrix_path)
    matrix_df.set_index(['src', 'dst'], inplace=True)
    # Преобразуем числовые столбцы в float
    numeric_cols = ['distance', 'time', 'price_per_km', 'price']
    for col in numeric_cols:
        if col in matrix_df.columns:
            matrix_df[col] = pd.to_numeric(matrix_df[col], errors='coerce')

    # Загрузка информации об офисах (перегрузных узлах)
    offices_df = None
    if os.path.exists(offices_path):
        offices_df = pd.read_csv(offices_path)
        offices_df.set_index('office_id', inplace=True)
        # Преобразуем числовые столбцы
        offices_df['transfer_price'] = pd.to_numeric(offices_df['transfer_price'], errors='coerce')
        offices_df['transfer_max'] = pd.to_numeric(offices_df['transfer_max'], errors='coerce')
        logger.info(f"✅ Загружено {len(offices_df)} офисов с информацией о перегрузке")
    else:
        logger.warning("⚠️ Файл offices.csv не найден. Перегрузки не будут учтены.")

    return df, matrix_df, offices_df


def load_solution(solution_file):
    """Загружает решение из JSON или CSV."""
    _, ext = os.path.splitext(solution_file)
    if ext.lower() == '.json':
        with open(solution_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    elif ext.lower() == '.csv':
        # Если CSV — ожидаем, что это paths.csv
        df = pd.read_csv(solution_file)
        # Предполагаем, что CSV содержит: src, dst, volume, path_nodes (в виде строки)
        flows = []
        for _, row in df.iterrows():
            try:
                path_nodes = eval(row['path_nodes'])  # Осторожно: только если доверяете источнику
                legs = [
                    {"from_office_id": path_nodes[i], "to_office_id": path_nodes[i+1]}
                    for i in range(len(path_nodes)-1)
                ]
                flows.append({
                    "src_office_id": row['src'],
                    "dst_office_id": row['dst'],
                    "avg_day_polybox_qty": row['volume'],
                    "legs": legs
                })
            except Exception as e:
                logger.error(f"Ошибка обработки строки в CSV: {e}")
        return {"flows": flows}
    else:
        raise ValueError(f"Неподдерживаемое расширение: {ext}")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Валидатор решения магистальной логистики в постановке MCF.\n"
                    "Проверяет ограничения и считает метрики:\n"
                    "1. Стоимость решения\n"
                    "2. Среднее время перемещения единицы объема груза\n"
                    "3. Среднее время исполнения заявки\n"
                    "4. Стоимость перевозки кубометра\n"
                    "5. Процент загруженности транспорта\n"
                    "6. Количество плеч с загрузкой < 50%\n"
                    "7. Стоимость перегруза и ограничения узлов"
    )
    parser.add_argument('-i', '--input-dir', type=str, required=True,
                        help='Директория с reqs.csv, distance_matrix.csv и offices.csv')
    parser.add_argument('-s', '--solution-file', type=str, required=True,
                        help='Файл решения: JSON или CSV (paths.csv)')
    parser.add_argument('--vehicle-capacity', type=float, default=90.0,
                        help='Вместимость ТС в м³ (по умолчанию 90.0)')
    
    parser.add_argument('--name', type=str, required=True,
                        help='Имя решения')
    return parser.parse_args()


def main():
    args = parse_arguments()

    # Загрузка данных
    df_tare, matrix_df, offices_df = load_data(args.input_dir)
    solution_data = load_solution(args.solution_file)

    # Загрузка решения
    solution = Solution.load_from_external_format(
        data=solution_data,        
        matrix_df=matrix_df,
        offices_df=offices_df,
        vehicle_capacity=args.vehicle_capacity
    )

    # Валидация: все ли заявки учтены?
    non_covered_reqs = solution.validate_coverage(df_tare)
    if non_covered_reqs:
        logger.warning("Неполностью покрытые заявки:")
        for err in non_covered_reqs:
            logger.warning(f"   {err}")
    else:
        logger.info("✅ Все заявки корректно учтены.")

    # Вывод отчёта
    print("\n📊 РЕЗУЛЬТАТ ВАЛИДАЦИИ:")
    print(solution.detailed_report(args.name))


if __name__ == '__main__':
    main()