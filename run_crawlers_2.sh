#!/bin/sh

#set -e

offset=0
while true; do
    date +"%Y-%m-%d_%H-%M-%S: next iteration, offset=$offset"
    ps -ef | grep scrapy | cut -c9-15 | xargs kill
    ps -ef | grep scrapy | cut -c9-15 | xargs kill
    #cat log/newest/pids | xargs kill
    #cat log/newest/pids | xargs kill
    sh run_crawlers.sh $offset
    offset=$(( $offset + 1 ))
    sleep 60m
    #sh run_rollups.sh
    #sh run_views.sh
    #psql novichenkobot -c 'refresh materialized view hostname_productivity'
    #sleep 30m
done
