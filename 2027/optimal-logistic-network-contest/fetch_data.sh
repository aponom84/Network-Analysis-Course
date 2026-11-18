#!/usr/bin/env bash
# Загружает открытый корпус экземпляров (Zenodo, CC BY 4.0) в каталог data/.
# Kovaleva V., Ponomarenko A. A Benchmark Generator of Realistic Logistic Transport
# Networks with Nonlinear Edge Costs and Transshipment Capacities. arXiv:2606.20653.
set -euo pipefail
cd "$(dirname "$0")"
URL="https://zenodo.org/records/20372456/files/logistic-network-benchmark-v1.0.0.zip?download=1"
curl -L --fail -o benchmark.zip "$URL"
mkdir -p data
unzip -q -o benchmark.zip -d data
rm benchmark.zip
echo "Готово: $(ls data/Synthetic | wc -l | tr -d ' ') синтетических и $(ls data/Real_Graph | wc -l | tr -d ' ') реальных сетей в data/"
