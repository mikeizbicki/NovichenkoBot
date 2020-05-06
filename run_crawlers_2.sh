#!/bin/sh

# run like a deamon
if [ "$1" != 'daemon_mode' ]; then

    # find and kill previous daemon
    pids=$(ps -ef | grep 'run_crawlers_2.sh daemon_mode' | grep -v grep | cut -c9-15)
    kill $pids > /dev/null 2>&1 || true

    # launch in daemon mode and exit non-daemon mode
    nohup $0 'daemon_mode' >> log/run_crawlers_2 2>&1 &
    exit
fi

# the main loop
iteration=0
while true; do

    # log time and iteration number
    date +"%Y-%m-%d_%H-%M-%S: next iteration, iteration=$iteration"

    # get pids of previous crawlers to cancel them later
    # by waiting to cancel them, we get less downtime if it takes a while to start new crawlers
    prev_pids=$(ps -ef | grep scrapy | cut -c9-15)

    # launch new crawlers
    sh run_crawlers.sh $offset
    offset=$(( $offset + 1 ))

    # kill previous crawlers
    kill $prev_pids > /dev/null 2>&1 || true
    kill $prev_pids > /dev/null 2>&1 || true

    # sleep
    sleep 20m
done
