#!/bin/bash

set -e

########################################
# settings
########################################

db=novichenkobot
db_rfc=postgres:///$db
num_jobs=80

########################################
# create log directories
########################################

log=log/$(date +'%Y-%m-%d-%H-%M-%S')
mkdir -p $log
ln -sfn "$(pwd)/$log" log/newest

########################################
# launch the high priority crawls
########################################

res=$(psql $db -c "select hostname from crawlable_hostnames where priority='high' order by hostname;")
hostnames_high=$(echo "$res" | tail -n +3 | head -n -1)

for hostname in $hostnames_high; do
    echo "high priority crawl: $hostname"
    nohup scrapy crawl general -s HOSTNAME_RESTRICTIONS=$hostname -a db=$db_rfc > $log/general-$hostname 2>&1 &
    echo $! >> $log/pids
done

########################################
# launch the low priority crawls
########################################

res=$(psql $db -c "select hostname from crawlable_hostnames where priority!='high' order by hostname;")
hostnames_low=$(echo "$res" | tail -n +3 | head -n -1)

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
