#!/bin/sh

set -e

export PYTHON_UNBUFFERED=True

id_articles0=000000000
step=20000000

for device in 0 1 2 3 4 5 6 7; do
    CUDA_VISIBLE_DEVICES=$device nice -n -1 nohup python3 Novichenko/Analyze/bert.py --id_articles0=$id_articles0 >> nohup/bert.$device &
    id_articles0=$(($id_articles0 + $step))
done
