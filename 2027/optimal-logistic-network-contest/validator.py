#!/usr/bin/env python3
"""Проверка решения конкурса 2027 и вычисление его стоимости.

Модель — путевая постановка из статьи Kovaleva & Ponomarenko (GVNS, 2026):

    min  sum_e c_e * y_e  +  sum_k sum_p sum_{v in p, v != s_k, t_k} f_v * x^k_p
    y_e = ceil( sum_{p : e in p} x_p / C )                 (число машин на ребре)
    sum_p x^k_p = d_k                                     (спрос выполнен)
    sum_{k, p : v in p, v != s_k, t_k} x^k_p <= W_v       (ёмкость перегруза склада)

Формат экземпляра (каталог корпуса Zenodo 10.5281/zenodo.20372456):
    offices.csv          office_id, transfer_price (f_v), transfer_max (W_v)
    reqs.csv             src, dst, volume (s_k, t_k, d_k)
    distance_matrix.csv  src, dst, price (c_e — стоимость рейса одной машины)

Формат решения (CSV, как result.csv в репозитории Optimal-Logistic-Net):
    src, dst, volume, path_nodes
    path_nodes — список номеров складов в квадратных скобках, от src до dst.
    Одна заявка может быть разделена на несколько путей.

Пример:
    python validator.py -i data/Real_Graph/Belgium -s belgium.csv
    python validator.py -i data/Real_Graph/Belgium -s belgium.csv --json
"""

import argparse
import ast
import csv
import json
import math
import sys
from collections import defaultdict

VEHICLE_CAPACITY = 50.0  # C, как в вычислительном эксперименте статьи (не data.txt!)
EPS = 1e-4  # допуск по объёму: спрос, ёмкость склада, загрузка машины
            # (50.00000000000001 из-за сложения вещественных чисел — одна машина)


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_instance(folder):
    offices = {}
    for r in read_csv(f"{folder}/offices.csv"):
        offices[int(r["office_id"])] = (float(r["transfer_price"]), float(r["transfer_max"]))
    demand = defaultdict(float)
    for r in read_csv(f"{folder}/reqs.csv"):
        demand[(int(r["src"]), int(r["dst"]))] += float(r["volume"])
    price = {}
    for r in read_csv(f"{folder}/distance_matrix.csv"):
        s, t, c = int(r["src"]), int(r["dst"]), float(r["price"])
        if s != t and c > 0:
            price[(s, t)] = c
    return offices, dict(demand), price


def load_solution(path):
    rows = []
    for i, r in enumerate(read_csv(path), start=2):
        nodes = [int(v) for v in ast.literal_eval(r["path_nodes"])]
        rows.append((i, int(r["src"]), int(r["dst"]), float(r["volume"]), nodes))
    return rows


def evaluate(offices, demand, price, rows, C=VEHICLE_CAPACITY):
    errors = []
    shipped = defaultdict(float)
    edge_flow = defaultdict(float)
    transit = defaultdict(float)
    reload_cost = 0.0

    for line, s, t, vol, nodes in rows:
        if vol < 0:
            errors.append(f"строка {line}: отрицательный объём {vol}")
            continue
        if (s, t) not in demand:
            errors.append(f"строка {line}: заявки {s}->{t} нет в reqs.csv")
            continue
        if len(nodes) < 2 or nodes[0] != s or nodes[-1] != t:
            errors.append(f"строка {line}: путь {nodes} не ведёт из {s} в {t}")
            continue
        if len(set(nodes)) != len(nodes):
            errors.append(f"строка {line}: путь {nodes} содержит цикл")
            continue
        bad = [(a, b) for a, b in zip(nodes, nodes[1:]) if (a, b) not in price]
        if bad:
            errors.append(f"строка {line}: рёбер {bad} нет в distance_matrix.csv")
            continue
        unknown = [v for v in nodes if v not in offices]
        if unknown:
            errors.append(f"строка {line}: складов {unknown} нет в offices.csv")
            continue
        shipped[(s, t)] += vol
        for e in zip(nodes, nodes[1:]):
            edge_flow[e] += vol
        for v in nodes[1:-1]:
            transit[v] += vol
            reload_cost += offices[v][0] * vol

    for k, d in demand.items():
        got = shipped.get(k, 0.0)
        if abs(got - d) > EPS:
            errors.append(f"заявка {k[0]}->{k[1]}: перевезено {got:.6f} из {d:.6f}")

    for v, load in transit.items():
        cap = offices[v][1]
        if load > cap + EPS:
            errors.append(f"склад {v}: транзит {load:.4f} > W_v = {cap}")

    vehicles = {e: math.ceil((f - EPS) / C) for e, f in edge_flow.items() if f > EPS}
    vehicle_cost = sum(price[e] * n for e, n in vehicles.items())
    total_vehicles = sum(vehicles.values())
    return {
        "feasible": not errors,
        "cost": vehicle_cost + reload_cost,
        "vehicle_cost": vehicle_cost,
        "reload_cost": reload_cost,
        "vehicles": total_vehicles,
        "used_edges": len(vehicles),
        "fill_rate": sum(f for e, f in edge_flow.items() if e in vehicles) / (C * total_vehicles) if total_vehicles else 0.0,
        "max_storage_load": max((transit[v] / offices[v][1] for v in transit), default=0.0),
        "errors": errors,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-i", "--instance", required=True, help="каталог экземпляра")
    ap.add_argument("-s", "--solution", required=True, help="CSV-файл решения")
    ap.add_argument("-C", "--vehicle-capacity", type=float, default=VEHICLE_CAPACITY)
    ap.add_argument("--json", action="store_true", help="вывести результат в JSON")
    a = ap.parse_args()

    res = evaluate(*load_instance(a.instance), load_solution(a.solution), C=a.vehicle_capacity)
    if a.json:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        for e in res["errors"][:50]:
            print("ОШИБКА:", e)
        if len(res["errors"]) > 50:
            print(f"... и ещё {len(res['errors']) - 50} ошибок")
        print(f"Допустимо:            {'да' if res['feasible'] else 'НЕТ'}")
        print(f"Стоимость:            {res['cost']:.2f}")
        print(f"  перевозки (машины): {res['vehicle_cost']:.2f}")
        print(f"  перегруз:           {res['reload_cost']:.2f}")
        print(f"Машин / рёбер:        {res['vehicles']} / {res['used_edges']}")
        print(f"Заполнение машин:     {100 * res['fill_rate']:.1f}%")
        print(f"Макс. загрузка склада: {100 * res['max_storage_load']:.1f}%")
    sys.exit(0 if res["feasible"] else 1)


if __name__ == "__main__":
    main()
