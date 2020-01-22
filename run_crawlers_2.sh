#!/bin/sh

#set -e

while true; do
    date +"%Y-%m-%d_%H-%M-%S: next iteration"
    cat log/newest/pids | xargs kill
    cat log/newest/pids | xargs kill
    sh run_crawlers.sh
    sleep 120m
    #sh run_rollups.sh
    #sh run_views.sh
    #psql novichenkobot -c 'refresh materialized view hostname_productivity'
    #sleep 30m
done
