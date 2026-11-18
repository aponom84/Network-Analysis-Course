#!/usr/bin/env python3
"""Упаковывает готовые решения в одну посылку для Яндекс.Контеста.

В Контесте задача устроена как обычная задача с чекером: программа участника
получает на stdin экземпляр (первая строка — «name <имя сети>») и печатает решение
в формате CSV. Чтобы сдать заранее посчитанные решения, этот скрипт собирает их
в один файл на Python 3: решения сжимаются и встраиваются в код, а программа
по имени сети печатает нужное.

Пример:
    python pack_submission.py -s my_solutions -o submission.py
    # my_solutions/<имя сети>.csv, например Belgium.csv, 50_nodes_3.csv

Сети, для которых нет файла, получат 0 баллов. Отправляйте submission.py
в Контест с компилятором Python 3.
"""

import argparse
import base64
import glob
import json
import os
import zlib

TEMPLATE = '''# Посылка, собранная pack_submission.py: {count} решений.
import base64, json, sys, zlib
DATA = {data!r}
name = sys.stdin.readline().split(None, 1)[1].strip()
sys.stdout.write(json.loads(zlib.decompress(base64.b85decode(DATA)).decode()).get(name, ""))
'''


def build(solutions_dir):
    sols = {}
    for path in sorted(glob.glob(os.path.join(solutions_dir, "*.csv"))):
        with open(path, encoding="utf-8") as f:
            sols[os.path.splitext(os.path.basename(path))[0]] = f.read()
    blob = base64.b85encode(zlib.compress(json.dumps(sols).encode(), 9)).decode()
    return TEMPLATE.format(count=len(sols), data=blob), sorted(sols)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-s", "--solutions", required=True, help="каталог с <имя сети>.csv")
    ap.add_argument("-o", "--output", default="submission.py")
    a = ap.parse_args()
    code, names = build(a.solutions)
    with open(a.output, "w", encoding="utf-8") as f:
        f.write(code)
    print(f"{a.output}: {len(names)} решений, {len(code) / 1024:.0f} КБ")


if __name__ == "__main__":
    main()
