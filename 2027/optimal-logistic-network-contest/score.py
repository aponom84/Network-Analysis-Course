#!/usr/bin/env python3
"""Итоговый балл участника по всем сетям открытого набора.

Ожидается каталог решений с файлами <имя сети>.csv (Belgium.csv, 50_nodes_3.csv, ...).
Балл за сеть: s_i = baseline_i / cost_i для допустимого решения, иначе 0.
Итоговый балл S — среднее по группам A, B, C (внутри группы — среднее по сетям).
S > 1 означает, что бейзлайн преодолён.

Пример:
    python score.py -d data -s my_solutions
    python score.py -d data -s my_solutions --csv my_scores.csv
"""

import argparse
import csv
import os
from collections import defaultdict

from validator import evaluate, load_instance, load_solution

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-d", "--data", default=os.path.join(HERE, "data"), help="каталог корпуса")
    ap.add_argument("-s", "--solutions", required=True, help="каталог с <имя сети>.csv")
    ap.add_argument("--reference", default=os.path.join(HERE, "reference_values.csv"))
    ap.add_argument("--csv", help="сохранить баллы по сетям в CSV")
    a = ap.parse_args()

    rows = []
    for ref in csv.DictReader(open(a.reference, encoding="utf-8")):
        sol = os.path.join(a.solutions, ref["name"] + ".csv")
        cost, s, status = None, 0.0, "не сдано"
        if os.path.exists(sol):
            try:
                res = evaluate(*load_instance(os.path.join(a.data, ref["group"], ref["name"])),
                               load_solution(sol))
                cost = res["cost"]
                if res["feasible"]:
                    s, status = float(ref["baseline"]) / cost, "допустимо"
                else:
                    status = f"НЕДОПУСТИМО ({len(res['errors'])} нарушений)"
            except Exception as e:  # битый файл не должен ронять подсчёт остальных
                status = f"ОШИБКА ЧТЕНИЯ: {e}"
        rows.append({"tier": ref["tier"], "name": ref["name"], "baseline": ref["baseline"],
                     "cost": f"{cost:.2f}" if cost is not None else "", "score": f"{s:.5f}",
                     "status": status})

    by_tier = defaultdict(list)
    for r in rows:
        by_tier[r["tier"]].append(float(r["score"]))
    tier_mean = {t: sum(v) / len(v) for t, v in sorted(by_tier.items())}
    total = sum(tier_mean.values()) / len(tier_mean)

    print(f"{'группа':<7}{'сеть':<17}{'бейзлайн':>15}{'стоимость':>15}{'балл':>9}  статус")
    for r in rows:
        print(f"{r['tier']:<7}{r['name']:<17}{float(r['baseline']):>15.2f}{r['cost']:>15}"
              f"{float(r['score']):>9.4f}  {r['status']}")
    print()
    for t, m in tier_mean.items():
        print(f"Группа {t}: {m:.5f}  (сетей: {len(by_tier[t])})")
    print(f"Итоговый балл S = {total:.5f}  —  бейзлайн {'ПРЕОДОЛЁН' if total > 1 else 'не преодолён'}")

    if a.csv:
        with open(a.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)


if __name__ == "__main__":
    main()
