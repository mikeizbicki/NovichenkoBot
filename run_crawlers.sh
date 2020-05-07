#!/bin/sh

#set -e
echo --------------------------------------------------------------------------------
date
echo --------------------------------------------------------------------------------

cd /home/mizbicki/NovichenkoBot
export PATH=/home/mizbicki/.local/bin:/home/mizbicki/.local/bin:/home/mizbicki/.cabal/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games

########################################
# settings
########################################

db=novichenkobot
db_rfc=postgres:///$db
num_jobs=150
hostnames_per_job=3

########################################
# create log directories
########################################

log=log/scrapy/$(date +'%Y-%m-%d-%H-%M-%S')
mkdir -p $log
ln -sfn "$(pwd)/$log" log/newest

########################################
# reset postgres stats monitors
########################################

#psql -c 'select pg_stat_reset ();' > /dev/null
#psql -c 'select pg_stat_statements_reset ();' > /dev/null

########################################
# kill all currently running crawls
########################################

#(ps -ef | grep scrapy | cut -c9-15 | xargs kill 2>/dev/null) || true
#(ps -ef | grep scrapy | cut -c9-15 | xargs kill 2>/dev/null) || true

########################################
# launch the high priority crawls
########################################

res=$(psql $db -c "select hostname from hostnames where priority='high' order by hostname;")
hostnames_high=$(echo "$res" | tail -n +4 | head -n -3)
for hostname in $hostnames_high; do
    echo "high priority crawl: $hostname"
    nohup nice -n -10 scrapy crawl general -s HOSTNAME_RESTRICTIONS=$hostname -s MEMQUEUE_LIMIT=200 -a db=$db_rfc > $log/general-$hostname 2>&1 &
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

# NOTE: started focusing on new languages when max(id_frontier)=618645536
res=$(psql $db -c "
SELECT hostname
--SELECT hostname, priority, count
FROM (
    SELECT hostname, max(priority) as priority, count(*) as count
    FROM (
        SELECT remove_www_from_hostname(substring(reverse(hostname_reversed) from 2)) as hostname,priority
        FROM frontier
        WHERE 
            timestamp_processed is null
            and substring(reverse(hostname_reversed) from 2) not in (
                SELECT hostname FROM hostnames WHERE priority in ('ban','high')
            )
            and not reverse(hostname_reversed) like any (array[
                '.m.%',
                '.mobile.%',
                '.amp.%',
                '.www.m.%',
                '.www.mobile.%',
                '.www.amp.%',
                '%.pinterest.%',
                '%.facebook.com',
                '%.wikipedia.%'
                ])
            --AND priority = float 'infinity'
        ORDER BY priority DESC
        LIMIT 50000
    )t
    WHERE hostname ~ '^[a-zA-Z0-9\-\.]+$'
    GROUP BY hostname
)t
ORDER BY priority DESC, count DESC, random()
limit $(($num_jobs * $hostnames_per_job))
;
")

# log the hostnames 
hostnames_low=$(echo "$res" | tail -n +4 | head -n -3)
i=0
for hostname in $hostnames_low; do
    echo ${hostname} >> $log/hostnames.$(printf "%04d" $(( i % $num_jobs )) )
    i=$(( $i + 1 ))
done

# launch low priority crawlers
i=0
for file in $log/hostnames.*; do
    restrictions=$(cat $log/hostnames.$(printf "%04d" $i) | xargs echo | tr ' ' ',')
    echo $i : $restrictions
    nohup nice -n 19 scrapy crawl general -s CONCURRENT_REQUESTS=32 -s CONCURRENT_REQUESTS_PER_DOMAIN=16 -s HOSTNAME_RESTRICTIONS=$restrictions -a db=$db_rfc > $log/general.$(printf "%04d" $i) 2>&1 &
    echo $! >> $log/pids
    i=$(( i + 1 ))
done
