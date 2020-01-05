#!/bin/sh

#set -e

while true; do
    date +"%Y-%m-%d_%H-%M-%S: next iteration"
    cat log/newest/pids | xargs kill
    cat log/newest/pids | xargs kill
    sh run_crawlers.sh
    sleep 90m
    sh run_rollups.sh
    sh run_views.sh
    sleep 30m
done
