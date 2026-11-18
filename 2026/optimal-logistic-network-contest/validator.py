import argparse
import os
import csv


def get_graph_from_df(file_path):
    # graph[a][b] - цена проезда по дороге (a,b)
    graph = {}
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            a = int(row[0])
            b = int(row[1])
            c = float(row[5])
            if a not in graph:
                graph[a] = {}
            graph[a][b] = c
    return graph


def get_query_from_df(file_path):
    # query[a][b] - количество товара из пункта a в пункт b
    query = {}
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            a = int(row[0])
            b = int(row[1])
            c = int(row[2])
            if a not in query:
                query[a] = {}
            query[a][b] = c
    return query


def get_transfer_data_from_df(file_path):
    transfer_price = {}
    transfer_max = {}
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            a = int(row[2])
            b = float(row[3])
            c = float(row[4])
            transfer_price[a] = b
            transfer_max[a] = c
    return transfer_price, transfer_max


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-dir', type=str, required=True,
                        help='Директория с reqs.csv, distance_matrix.csv и offices.csv')
    parser.add_argument('-s', '--solution-file', type=str, required=True,
                        help='Csv файл с решением')
    return parser.parse_args()


def main():
    args = parse_arguments()

    # читаем csv
    graph = get_graph_from_df(os.path.join(
        args.input_dir, 'distance_matrix.csv'))
    query = get_query_from_df(os.path.join(args.input_dir, 'reqs.csv'))
    transfer_price, transfer_max = get_transfer_data_from_df(
        os.path.join(args.input_dir, 'offices.csv'))

    # собираем все вершины
    all_v = set()
    for a in graph:
        all_v.add(a)
        for b in graph[a]:
            all_v.add(b)

    # анализируем решение
    query_vol = {}
    capacity = {}
    transfer = {}
    peregruz_sum = 0
    cars_sum = 0

    with open(args.solution_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            src = int(row[0])
            dst = int(row[1])
            volume = float(row[2])
            path = row[3][1:-1]
            path = [int(i) for i in path.split(',')]

            for v in path:
                assert v in all_v, \
                    f"Не существует вершины {v}!"

            assert volume >= 0, \
                f"Количество груза на пути {path} должно быть неотрицательным числом!"

            if src not in query_vol:
                query_vol[src] = {}

            if dst not in query_vol[src]:
                query_vol[src][dst] = 0

            query_vol[src][dst] += volume

            for i in range(len(path)-1):
                a = path[i]
                b = path[i+1]

                assert (a in graph) and (b in graph[a]), \
                    f"Ребра ({a}, {b}) нет в графе!"

                if a not in capacity:
                    capacity[a] = {}
                if b not in capacity[a]:
                    capacity[a][b] = 0
                capacity[a][b] += volume

            for v in path[1:-1]:
                peregruz_sum += transfer_price[v] * volume
                if v not in transfer:
                    transfer[v] = 0
                transfer[v] += volume

        for a in capacity:
            for b in capacity[a]:
                volume = round(capacity[a][b], 0)
                volume = int(volume)
                cars = (volume // 90) + (volume % 90 != 0)
                cars_sum += cars * graph[a][b]

        for v in transfer:
            assert int(round(transfer[v], 0)) <= int(round(transfer_max[v], 0)), \
                f"В вершине {v} нарушено ограничение перегруза!"

        for a in query:
            for b in query[a]:
                assert int(round(query[a][b], 0)) == int(round(query_vol[a][b], 0)), \
                    f"Для запроса {a}->{b} не доставлено требуемое количество груза!"

        print(f"Значение целевой функции: {peregruz_sum+cars_sum}")


if __name__ == '__main__':
    main()
