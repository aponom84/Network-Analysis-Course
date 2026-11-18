#!/usr/bin/env python3
"""Собирает пакет задачи в формате Polygon для импорта в Яндекс.Контест.

    python make_package.py                         # тесты из ../data, без эталонного решения
    python make_package.py --main-solutions DIR    # DIR/<имя сети>.csv — главное решение пакета

Результат: build/contest/ (contest.xml + problems/logistic-net-2027/) и два архива:
build/logistic-net-2027-contest.zip — пакет соревнования, build/logistic-net-2027-problem.zip — пакет задачи.
Перед сборкой загрузите корпус: ../fetch_data.sh.
"""

import argparse
import csv
import os
import shutil
import subprocess
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
from pack_submission import build as pack  # noqa: E402

SHORT = "logistic-net-2027"
CONTEST_TITLE = "Конкурс 2027: проектирование логистической сети"
# Пакет собран не в Polygon, поэтому адреса условные; последний сегмент совпадает
# с каталогом задачи problems/<SHORT>, как в пакетах, которые выгружает Polygon.
PROBLEM_URL = f"https://polygon.codeforces.com/p0000000/aponom84/{SHORT}"
CONTEST_URL = "https://polygon.codeforces.com/c/0"
TITLE = "Проектирование логистической сети"
CAPACITY = 50
TIME_LIMIT_MS = 10000
MEMORY_LIMIT = 512 * 1024 * 1024


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_test(inst_dir, name, out_path):
    offices = read_csv(f"{inst_dir}/offices.csv")
    reqs = read_csv(f"{inst_dir}/reqs.csv")
    edges = [r for r in read_csv(f"{inst_dir}/distance_matrix.csv")
             if r["src"] != r["dst"] and float(r["price"]) > 0]
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(f"name {name}\ncapacity {CAPACITY}\n")
        f.write(f"offices {len(offices)}\n")
        f.writelines(f"{r['office_id']} {r['transfer_price']} {r['transfer_max']}\n" for r in offices)
        f.write(f"edges {len(edges)}\n")
        f.writelines(f"{r['src']} {r['dst']} {r['price']}\n" for r in edges)
        f.write(f"requests {len(reqs)}\n")
        f.writelines(f"{r['src']} {r['dst']} {r['volume']}\n" for r in reqs)


SECTIONS = {
    "name.tex": TITLE,
    "legend.tex": r"""
Дан полный ориентированный граф: вершины --- склады, рёбра --- перевозки между ними.
Для склада $v$ заданы стоимость перегруза единицы груза $f_v$ и ёмкость перегруза $W_v$,
для ребра $e$ --- стоимость рейса одной машины $c_e$; вместимость машины $C = 50$.
Заявка $k$ требует перевезти объём $d_k$ со склада $s_k$ на склад $t_k$; заявку можно
разделить на несколько путей.

Требуется выбрать для каждой заявки пути $p$ и объёмы $x^k_p \ge 0$, $\sum_p x^k_p = d_k$,
так, чтобы транзитный груз через каждый склад $v$ (груз заявок, для которых $v$ не является
ни $s_k$, ни $t_k$) не превышал $W_v$, и минимизировать стоимость
$$\sum_{e} c_e \left\lceil \frac{1}{C}\sum_{p \ni e} x_p \right\rceil
 + \sum_{k}\sum_{p}\sum_{v \in p,\ v \ne s_k, t_k} f_v\, x^k_p.$$

Задача и 50 сетей взяты из работ V.~Kovaleva, A.~Ponomarenko (arXiv:2606.20653, arXiv:2610.30139).
Сети, валидатор, программа подсчёта баллов и упаковщик посылки:
каталог \texttt{2027/optimal-logistic-network-contest} репозитория курса.
""",
    "input.tex": r"""
Первая строка --- \texttt{name} и имя сети (может содержать пробел, например \texttt{Europe Russia}).
Вторая строка --- \texttt{capacity 50}.
Далее \texttt{offices $N$} и $N$ строк <<\texttt{id} $f_v$ $W_v$>>;
\texttt{edges $M$} и $M$ строк <<\texttt{src dst} $c_e$>>;
\texttt{requests $K$} и $K$ строк <<\texttt{src dst} $d_k$>>.
Те же данные лежат в открытом корпусе в формате CSV.
""",
    "output.tex": r"""
Выведите решение в формате CSV (строка заголовка необязательна):

\texttt{src,dst,volume,path\_nodes}\\
\texttt{2,9,10.12,"[2, 4, 9]"}\\
\texttt{2,9,15.84,"[2, 1, 9]"}

Каждая строка --- часть заявки \texttt{src}$\to$\texttt{dst} объёмом \texttt{volume},
перевозимая по пути \texttt{path\_nodes} без повторов вершин.

Посчитанные заранее решения удобно сдавать одной программой: скрипт \texttt{pack\_submission.py}
собирает каталог \texttt{<имя сети>.csv} в файл на Python~3, который печатает решение для сети
из первой строки ввода.
""",
    "scoring.tex": r"""
Объёмы сравниваются с допуском $10^{-4}$. Для допустимого решения сети $i$ балл
$s_i = B_i / \mathrm{cost}_i$, где $B_i$ --- бейзлайн: $0{,}99$ от лучшего результата GVNS,
а на 9 сетях, где $0{,}99 \cdot \mathrm{GVNS}_i$ ниже доказанной нижней оценки, --- сам GVNS.
Недопустимое решение получает $s_i = 0$ (вердикт OK, причина в комментарии к тесту).

За тест начисляется $100\, w_i\, s_i$, где $w_i = 1/(3\,|G_i|)$, $G_i$ --- группа сети:
A ($|V| \le 24$, 23 сети), B ($28 \le |V| \le 59$, 20 сетей), C ($|V| \ge 90$, 7 сетей).
Сумма по всем тестам равна $100\,S$; бейзлайн преодолён при сумме больше 100.
""",
}


def problem_tex():
    s = SECTIONS
    return (f"\\begin{{problem}}{{{TITLE}}}{{стандартный ввод}}{{стандартный вывод}}"
            f"{{{TIME_LIMIT_MS // 1000} секунд}}{{{MEMORY_LIMIT >> 20} мегабайт}}\n\n"
            f"{s['legend.tex']}\n\\InputFile\n{s['input.tex']}\n\\OutputFile\n{s['output.tex']}\n"
            f"\\Scoring\n{s['scoring.tex']}\n\\end{{problem}}\n")


def problem_html():
    import html as H
    def para(t):
        t = t.strip().replace("---", "—").replace("<<", "«").replace(">>", "»").replace("~", " ")
        t = t.replace("\\texttt{", "<code>").replace("\\_", "_").replace("\\\\", "<br>")
        out, depth = [], 0
        for ch in t:  # закрываем <code> на парной фигурной скобке
            if ch == "{":
                depth += 1
            elif ch == "}" and depth == 0:
                out.append("</code>")
                continue
            elif ch == "}":
                depth -= 1
            out.append(ch)
        return "".join(f"<p>{x}</p>" for x in "".join(out).split("\n\n"))
    s = SECTIONS
    return (f"<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>{TITLE}</title></head><body>"
            f"<h1>{TITLE}</h1>{para(s['legend.tex'])}<h2>Входные данные</h2>{para(s['input.tex'])}"
            f"<h2>Выходные данные</h2>{para(s['output.tex'])}<h2>Система оценки</h2>{para(s['scoring.tex'])}"
            "</body></html>\n")


def contest_xml():
    return f'''<?xml version="1.0" encoding="utf-8" standalone="no"?>
<contest url="{CONTEST_URL}">
    <names>
        <name language="russian" value="{CONTEST_TITLE}"/>
    </names>
    <statements/>
    <problems>
        <problem index="a" url="{PROBLEM_URL}"/>
    </problems>
</contest>
'''


def problem_xml(n_tests, main_solution):
    tests = "\n".join('        <test method="manual"/>' for _ in range(n_tests))
    sol = ""
    if main_solution:
        sol = ('    <solutions>\n      <solution tag="main">\n'
               '        <source path="solutions/main.py" type="python.3"/>\n'
               '      </solution>\n    </solutions>\n')
    return f'''<?xml version="1.0" encoding="utf-8" standalone="no"?>
<problem revision="1" short-name="{SHORT}" url="{PROBLEM_URL}">
  <names>
    <name language="russian" value="{TITLE}"/>
  </names>
  <statements>
    <statement charset="UTF-8" language="russian" mathjax="true" path="statements/russian/problem.tex" type="application/x-tex"/>
    <statement charset="UTF-8" language="russian" mathjax="true" path="statements/.html/russian/problem.html" type="text/html"/>
  </statements>
  <judging cpu-name="Intel(R) Core(TM) i3-8100 CPU @ 3.60GHz" cpu-speed="3600" input-file="" output-file="" run-count="1">
    <testset name="tests">
      <time-limit>{TIME_LIMIT_MS}</time-limit>
      <memory-limit>{MEMORY_LIMIT}</memory-limit>
      <test-count>{n_tests}</test-count>
      <input-path-pattern>tests/%02d</input-path-pattern>
      <answer-path-pattern>tests/%02d.a</answer-path-pattern>
      <tests>
{tests}
      </tests>
    </testset>
  </judging>
  <files>
    <resources>
      <file path="files/testlib.h" type="h.g++"/>
    </resources>
  </files>
  <assets>
    <checker name="check.cpp" type="testlib">
      <source path="files/check.cpp" type="cpp.g++17"/>
      <copy path="check.cpp"/>
      <testset>
        <test-count>0</test-count>
        <input-path-pattern>files/tests/checker-tests/%02d</input-path-pattern>
        <output-path-pattern>files/tests/checker-tests/%02d.o</output-path-pattern>
        <answer-path-pattern>files/tests/checker-tests/%02d.a</answer-path-pattern>
        <tests/>
      </testset>
    </checker>
{sol}  </assets>
  <properties>
    <property name="tests-wellformed" value="true"/>
  </properties>
  <stresses>
    <stress-count>0</stress-count>
    <stress-path-pattern>stresses/%03d</stress-path-pattern>
    <list/>
  </stresses>
  <tags>
    <tag value="optimization"/>
    <tag value="output-only"/>
  </tags>
</problem>
'''


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=os.path.join(ROOT, "data"))
    ap.add_argument("--main-solutions", help="каталог <имя сети>.csv для главного решения пакета")
    ap.add_argument("--out", default=os.path.join(HERE, "build"))
    a = ap.parse_args()

    ref = read_csv(os.path.join(ROOT, "reference_values.csv"))
    tier_size = Counter(r["tier"] for r in ref)
    contest = os.path.join(a.out, "contest")
    pkg = os.path.join(contest, "problems", SHORT)
    shutil.rmtree(a.out, ignore_errors=True)
    for d in ("tests", "files", "statements/russian", "statements/.html/russian",
              "statement-sections/russian", "solutions"):
        os.makedirs(os.path.join(pkg, d))

    for i, r in enumerate(ref, start=1):
        write_test(os.path.join(a.data, r["group"], r["name"]), r["name"], f"{pkg}/tests/{i:02d}")
        weight = 1.0 / (len(tier_size) * tier_size[r["tier"]])
        with open(f"{pkg}/tests/{i:02d}.a", "w", newline="\n") as f:
            f.write(f"{r['baseline']} {weight:.15g}\n")

    for f in ("check.cpp", "testlib.h"):
        shutil.copy(os.path.join(HERE, f), os.path.join(pkg, "files", f))
    shutil.copy(os.path.join(HERE, "check.cpp"), os.path.join(pkg, "check.cpp"))
    for fname, text in SECTIONS.items():
        with open(f"{pkg}/statement-sections/russian/{fname}", "w", encoding="utf-8") as f:
            f.write(text.strip() + "\n")
    with open(f"{pkg}/statements/russian/problem.tex", "w", encoding="utf-8") as f:
        f.write(problem_tex())
    if a.main_solutions:
        code, names = pack(a.main_solutions)
        with open(f"{pkg}/solutions/main.py", "w", encoding="utf-8") as f:
            f.write(code)
        print(f"главное решение: {len(names)} сетей")
    with open(f"{pkg}/statements/.html/russian/problem.html", "w", encoding="utf-8") as f:
        f.write(problem_html())
    with open(f"{pkg}/problem.xml", "w", encoding="utf-8") as f:
        f.write(problem_xml(len(ref), bool(a.main_solutions)))
    with open(f"{contest}/contest.xml", "w", encoding="utf-8") as f:
        f.write(contest_xml())

    # Два архива: соревнование («Загрузить соревнование» → POLYGON) и отдельная задача.
    for src, zname in ((contest, f"{SHORT}-contest.zip"), (pkg, f"{SHORT}-problem.zip")):
        zip_path = os.path.join(a.out, zname)
        subprocess.run(["zip", "-qr", "-X", zip_path, "."], cwd=src, check=True)
        print(f"{zip_path} ({os.path.getsize(zip_path) / 2**20:.1f} МБ)")
    print(f"{len(ref)} тестов")


if __name__ == "__main__":
    main()
