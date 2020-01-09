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
# launch the generic crawler
########################################

#for i in $(seq 0 0); do
    #echo "generic crawl: offset index = $i"
    #nohup nice -n -19 scrapy crawl general -s OFFSET_INDEX=$i -s ROBOTSTXT_OBEY=False -s MEMQUEUE_LIMIT=50000 -s DNS_TIMEOUT=15 -s DOWNLOAD_TIMEOUT=15 -s CONCURRENT_REQUESTS=128 -s REACTOR_THREADPOOL_MAXSIZE=64 -s AUTOTHROTTLE_ENABLED=False -a db=$db_rfc > $log/allhosts-$(printf "%04d" $i) 2>&1 &
    #echo $! >> $log/pids
#done

########################################
# launch the low priority crawls
########################################

#res=$(psql $db -c "
#select hostname 
#from crawlable_hostnames 
#where 
    #priority='' and 
    #(lang='de' or lang='fr')
#order by hostname;
#")
#res=$(psql $db -c "
#SELECT hostname
#FROM hostname_productivity
#ORDER BY priority desc
#limit 500;
#")
res=$(psql $db -c "
SELECT DISTINCT hostname
FROM (
    SELECT substring(reverse(hostname_reversed) from 2) as hostname,priority
    FROM frontier
    WHERE 
        timestamp_processed is null
        and substring(reverse(hostname_reversed) from 2) not in (
            SELECT hostname FROM crawlable_hostnames WHERE priority='ban'
        )
    ORDER BY priority DESC
    LIMIT 100000
)t
LIMIT 300
;
")
#res=$(psql $db -c "
#SELECT hostname 
#FROM frontier_hostname 
#WHERE num_0>0 AND hostname NOT IN (SELECT * FROM hostnames_responses) 
#ORDER BY num_1000000,num_100000,num_10000,num_1000,num_100,num_10,num_0
#LIMIT 500;
#")
#res=$(psql $db -c "
#SELECT DISTINCT hostname
#FROM articles_lang
#WHERE substring(hostname from '\.[^\.]+$') = '.fr' or substring(hostname from '\.[^\.]+$') = '.gov';
#")
#res=$(psql $db -c "
#select hostname_target from (
#select hostname_target,sum(num) as num
#from refs_keywords
#where
    #type='link' and
    #hostname_source in (select hostname from hostname_productivity limit 5000) and
    #hostname_target not in (select hostname from crawlable_hostnames) and
    #right(hostname_target, length(hostname_target)-4) not in (SELECT hostname FROM crawlable_hostnames) and
    #hostname_target not in (select distinct hostname from responses_timestamp_hostname)
#group by hostname_target
#order by num desc
#) as t1
#limit $(( 5 * $num_jobs ));
#")

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
