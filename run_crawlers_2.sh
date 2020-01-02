#!/bin/sh

#set -e

while true; do
    date +"%Y-%m-%d_%H-%M-%S: next iteration"
    cat log/newest/pids | xargs kill
    cat log/newest/pids | xargs kill
    sh run_crawlers.sh
    sleep 30m
    nohup python3 -u scripts/rollup.py --name=responses_timestamp_hostname --max_rollup_size=100000 >> nohup.rollup.responses_timestamp_hostname &
    sleep 30m
done
