"""Рисунки лекции «Оптимизация логистических сетей» в палитре ВШЭ.

    python make_figs.py /path/to/Optimal-Logistic-Net

Рисунки строятся скриптами доклада на «Импульсе» 2026 из репозитория
проекта (https://github.com/aponom84/Optimal-Logistic-Net, код MIT, данные
CC BY 4.0): presentations/impulse2026/make_figs.py и trace_figs.py.
Скрипт копирует их во временную папку, заменяет цвета и шрифт на цвета
брендбука ВШЭ и HSE Sans и пишет PDF и demo_numbers.tex в ./figs/.
Нужны только numpy и matplotlib: трассы прогонов GVNS уже лежат в
presentations/impulse2026/trace/ репозитория проекта.
"""
import glob
import os
import runpy
import shutil
import sys
import tempfile

from matplotlib import font_manager

HERE = os.path.dirname(os.path.abspath(__file__))

# старые цвета (tab10 и metropolis) -> палитра брендбука ВШЭ
COLORS = {
    '"#1f77b4"': '"#11A0D7"',  # LP-релаксация
    '"#ff7f0e"': '"#FBBA00"',  # случайный старт
    '"#2ca02c"': '"#029C63"',  # MILP-солвер
    '"#d62728"': '"#E61F3D"',  # отрыв от нижней оценки
    '"#4d4d4d"': '"#102D69"',  # GVNS
    '"#1F77B4"': '"#234A9B"',  # N1
    '"#E8710A"': '"#EB681F"',  # N2
    '"#B2182B"': '"#E61F3D"',  # N3, ёмкости
    '"#23373B"': '"#102D69"',  # контуры и подписи узлов
    '"#cfe3e3"': '"#CDDDF0"',  # заливка узлов
    '"#5b8db8"': '"#7DA0D3"',  # заявки
    '"#7a8b8f"': '"#7DA0D3"',  # машины на старте
    '["Fira Sans", ': '["HSE Sans", "Fira Sans", ',
}

# подписи рисунков и тексты примеров -- академическим языком
TEXT = {
    "MILP-солвер, сравнимое время": "MILP-решатель, сравнимое время",
    "отрыв GVNS от нижней оценки": "отклонение GVNS от нижней оценки",
    '"отрыв от нижней оценки, %"': '"отклонение от нижней оценки, %"',
    '"нижняя оценка солвера"': '"нижняя оценка решателя"',
    'label="было"': 'label="до хода"',
    '"предложен, не взят"': '"предложен, не выбран"',
    '"объезд с потоком"': '"обходной путь с потоком"',
    "ед.): ехала напрямую; после хода": "ед.) перевозилась напрямую; после хода",
    "ед. идут через склад": "ед. перевозятся через склад",
    "ед. уходят на путь": "ед. переходят на путь",
    "ед. уходят со склада": "ед. переводятся со склада",
    "склад {removed} убран": "склад {removed} исключён",
    "(старт GVNS)": "(начальное решение GVNS)",
}


def main(repo):
    src = os.path.join(repo, "presentations", "impulse2026")
    for f in glob.glob(os.path.expanduser("~/Library/Fonts/HSESans-*.otf")):
        font_manager.fontManager.addfont(f)
    tmp = tempfile.mkdtemp()
    try:
        for name in ("make_figs.py", "trace_figs.py"):
            text = open(os.path.join(src, name), encoding="utf-8").read()
            for old, new in {**COLORS, **TEXT}.items():
                text = text.replace(old, new)
            if name == "make_figs.py":
                text = text.replace(
                    'HERE = os.path.dirname(os.path.abspath(__file__))',
                    f'HERE = {src!r}')
                text = text.replace('OUT = os.path.join(HERE, "figs")',
                                    f'OUT = {os.path.join(HERE, "figs")!r}')
            open(os.path.join(tmp, name), "w", encoding="utf-8").write(text)
        sys.path.insert(0, tmp)
        runpy.run_path(os.path.join(tmp, "make_figs.py"), run_name="__main__")
    finally:
        shutil.rmtree(tmp)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/research/optima2026"))
