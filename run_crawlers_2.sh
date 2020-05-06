#!/bin/sh

#set -e

# run like a deamon
if [ "$1" != 'daemon_mode' ]; then

    # find and kill previous daemon
    pids=$(ps -ef | grep 'run_crawlers_2.sh daemon_mode' | grep -v grep | cut -c9-15)
    kill $pids > /dev/null 2>&1

    # launch in daemon mode and exit non-daemon mode
    nohup $0 'daemon_mode' >> log/run_crawlers_2 2>&1 &
    exit
fi

# the main loop
offset=0
while true; do
    date +"%Y-%m-%d_%H-%M-%S: next iteration, offset=$offset"
    prev_pids=$(ps -ef | grep scrapy | cut -c9-15)
    #select pid,application_name,pg_cancel_backend(pid) from pg_stat_activity where application_name='NovichenkoBot_GeneralSpider';
    #cat log/newest/pids | xargs kill
    #cat log/newest/pids | xargs kill
    sh run_crawlers.sh $offset
    offset=$(( $offset + 150 ))
    echo $prev_pids | xargs kill > /dev/null 2>&1
    echo $prev_pids | xargs kill > /dev/null 2>&1
    sleep 20m
    #sh run_rollups.sh
    #sh run_views.sh
    #psql novichenkobot -c 'refresh materialized view hostname_productivity'
    #sleep 30m
done
