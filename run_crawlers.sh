#!/bin/bash

set -e

########################################
# settings
########################################

db=novichenkobot
db_rfc=postgres:///$db
num_jobs=100

########################################
# create log directories
########################################

log=log/$(date +'%Y-%m-%d-%H-%M-%S')
mkdir -p $log
ln -sfn "$(pwd)/$log" log/newest

########################################
# reset postgres stats monitors
########################################

psql -c 'select pg_stat_reset ();' > /dev/null
psql -c 'select pg_stat_statements_reset ();' > /dev/null

########################################
# launch the high priority crawls
########################################

res=$(psql $db -c "select hostname from crawlable_hostnames where priority='high' order by hostname;")
hostnames_high=$(echo "$res" | tail -n +4 | head -n -3)

for hostname in $hostnames_high; do
    echo "high priority crawl: $hostname"
    nice -n -10 scrapy crawl general -s HOSTNAME_RESTRICTIONS=$hostname -a db=$db_rfc > $log/general-$hostname 2>&1 &
    echo $! >> $log/pids
done

########################################
# launch the low priority crawls
########################################

res=$(psql $db -c "
select hostname 
from crawlable_hostnames 
where 
    priority='' and 
    (lang='en' or lang='')
order by hostname;
")
res=$(psql $db -c "
select hostname_target from (
select hostname_target,sum(num) as num
from refs_keywords
where
    type='link' and
    hostname_source in (select hostname from hostname_productivity limit 500) and
    hostname_target not in (select hostname from crawlable_hostnames) and
    right(hostname_target, length(hostname_target)-4) not in (SELECT hostname FROM crawlable_hostnames) and
    hostname_target not in (select hostname from responses_timestamp_hostname)
group by hostname_target
order by num desc
) as t1
-- offset $(( 25 * $num_jobs ))
limit $(( 5 * $num_jobs ));
")

hostnames_low=$(echo "$res" | tail -n +4 | head -n -3)

i=0
for hostname in $hostnames_low; do
    echo ${hostname} >> $log/hostnames.$(printf "%04d" $(( i % $num_jobs )) )
    i=$(( $i + 1 ))
done

i=0
for file in $log/hostnames.*; do
    restrictions=$(cat $log/hostnames.$(printf "%04d" $i) | xargs echo | tr ' ' ',')
    echo $i : $restrictions
    nohup nice -n 19 scrapy crawl general -s HOSTNAME_RESTRICTIONS=$restrictions -a db=$db_rfc > $log/general.$(printf "%04d" $i) 2>&1 &
    echo $! >> $log/pids
    i=$(( i + 1 ))
done
