// Чекер конкурса 2027 «Проектирование логистической сети» (testlib).
//
// inf — экземпляр (формат см. make_package.py):
//     name <имя сети>
//     capacity <C>
//     offices <N>        затем N строк: id f_v W_v
//     edges <M>          затем M строк: src dst c_e
//     requests <K>       затем K строк: src dst d_k
// ouf — вывод участника: CSV «src,dst,volume,path_nodes», path_nodes = "[a, b, c]";
//       строка заголовка необязательна, пустые строки пропускаются.
// ans — «baseline weight»: бейзлайн B_i и вес сети w_i = 1 / (3 * |группа|).
//
// Баллы за тест: 100 * w_i * B_i / cost_i при допустимом решении, иначе 0.
// Сумма баллов по 50 тестам равна 100 * S, где S — итоговый балл регламента.
// Недопустимое решение получает вердикт OK с 0 баллов, чтобы тестирование
// продолжалось на остальных сетях; причина выводится в комментарии.
//
// Должен давать ту же стоимость, что validator.py (допуск EPS = 1e-4).

#include "testlib.h"

#include <cmath>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

using namespace std;

static const double EPS = 1e-4;

static void zero(const string &why) { quitp(0.0, "0 баллов: " + why); }

int main(int argc, char *argv[]) {
    registerTestlibCmd(argc, argv);

    // --- экземпляр ---
    inf.readToken();  // "name"
    string name = trim(inf.readLine());
    inf.readToken();
    double C = inf.readDouble();
    inf.readToken();
    int n = inf.readInt();
    map<long long, pair<double, double>> office;  // id -> (f_v, W_v)
    for (int i = 0; i < n; i++) {
        long long id = inf.readLong();
        double f = inf.readDouble();
        double w = inf.readDouble();
        office[id] = {f, w};
    }
    inf.readToken();
    int m = inf.readInt();
    map<pair<long long, long long>, double> price;
    for (int i = 0; i < m; i++) {
        long long s = inf.readLong(), t = inf.readLong();
        double c = inf.readDouble();
        price[{s, t}] = c;
    }
    inf.readToken();
    int k = inf.readInt();
    map<pair<long long, long long>, double> demand;
    for (int i = 0; i < k; i++) {
        long long s = inf.readLong(), t = inf.readLong();
        demand[{s, t}] += inf.readDouble();
    }
    double baseline = ans.readDouble();
    double weight = ans.readDouble();

    // --- решение участника ---
    map<pair<long long, long long>, double> shipped, edgeFlow;
    map<long long, double> transit;
    double reloadCost = 0;
    // Весь вывод читается заранее: при досрочном выходе с 0 баллов testlib
    // иначе выставил бы PE «Extra information in the output file».
    vector<string> lines;
    while (!ouf.eof()) lines.push_back(ouf.readLine());
    int line = 0;
    for (const string &raw : lines) {
        line++;
        string s;
        for (char ch : raw) s += (ch == ',' || ch == '[' || ch == ']' || ch == '"' || ch == '\r' || ch == '\t') ? ' ' : ch;
        if (trim(s).empty()) continue;
        if (line == 1 && raw.find("src") != string::npos) continue;  // заголовок
        istringstream is(s);
        long long src, dst;
        double vol;
        if (!(is >> src >> dst >> vol)) zero(format("строка %d: ожидается src,dst,volume,path_nodes", line));
        vector<long long> path;
        string tok;
        while (is >> tok) {
            char *end = nullptr;
            long long v = strtoll(tok.c_str(), &end, 10);
            if (*end != 0) zero(format("строка %d: «%s» не номер склада", line, tok.c_str()));
            path.push_back(v);
        }
        if (!(vol >= 0) || std::isinf(vol)) zero(format("строка %d: недопустимый объём", line));
        if (!demand.count({src, dst})) zero(format("строка %d: заявки %lld->%lld нет", line, src, dst));
        if (path.size() < 2 || path.front() != src || path.back() != dst)
            zero(format("строка %d: путь не ведёт из %lld в %lld", line, src, dst));
        if (set<long long>(path.begin(), path.end()).size() != path.size())
            zero(format("строка %d: путь содержит цикл", line));
        for (long long v : path)
            if (!office.count(v)) zero(format("строка %d: склада %lld нет", line, v));
        for (size_t i = 0; i + 1 < path.size(); i++)
            if (!price.count({path[i], path[i + 1]}))
                zero(format("строка %d: ребра %lld->%lld нет", line, path[i], path[i + 1]));
        shipped[{src, dst}] += vol;
        for (size_t i = 0; i + 1 < path.size(); i++) edgeFlow[{path[i], path[i + 1]}] += vol;
        for (size_t i = 1; i + 1 < path.size(); i++) {
            transit[path[i]] += vol;
            reloadCost += office[path[i]].first * vol;
        }
    }

    for (auto &d : demand) {
        double got = shipped.count(d.first) ? shipped[d.first] : 0.0;
        if (fabs(got - d.second) > EPS)
            zero(format("заявка %lld->%lld: перевезено %.4f из %.4f", d.first.first, d.first.second, got, d.second));
    }
    for (auto &t : transit)
        if (t.second > office[t.first].second + EPS)
            zero(format("склад %lld: транзит %.4f > W = %.4f", t.first, t.second, office[t.first].second));

    double vehicleCost = 0;
    long long vehicles = 0;
    for (auto &e : edgeFlow) {
        if (e.second <= EPS) continue;
        long long y = (long long)ceil((e.second - EPS) / C);
        vehicles += y;
        vehicleCost += price[e.first] * y;
    }
    double cost = vehicleCost + reloadCost;
    if (!(cost > 0)) zero("нулевая стоимость");
    double ratio = baseline / cost;
    quitp(100.0 * weight * ratio,
          format("%s: стоимость %.2f (машины %.2f, перегруз %.2f, машин %lld), бейзлайн %.2f, s = %.5f",
                 name.c_str(), cost, vehicleCost, reloadCost, vehicles, baseline, ratio));
}
