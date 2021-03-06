#!/bin/sh

set -e

cd /home/mizbicki/NovichenkoBot
logdir=log/rollup
mkdir -p $logdir

rollups="
responses_summary
"
# these are temporarily paused
#responses_timestamp_hostname
#articles_lang 
#articles_summary2 
#frontier_hostname
#requests_hostname

# don't do this one for real: 
# refs_summary

for rollup in $rollups; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') rollup=$rollup"
    nohup python3 -u Novichenko/Bot/rollup.py --name=$rollup --max_rollup_size=100000 >> $logdir/nohup.rollup.$rollup < /dev/null 2>&1 &
done

