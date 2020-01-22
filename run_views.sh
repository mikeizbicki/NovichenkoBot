#!/bin/sh

set -e

cd /home/mizbicki/NovichenkoBot
logdir=log/rollup
mkdir -p $logdir

views="
articles_lang_hostnames
articles_lang_stats
responses_timestamp_hostname_hostnames
responses_timestamp_hostname_recent
refs_summary_simple
hostname_progress
hostname_productivity
hostname_peryear
"

for view in $views; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') view=$view"
    psql novichenkobot -c "refresh materialized view concurrently $view;" >> $logdir/nohup.view.$view < /dev/null 2>&1 &
done

