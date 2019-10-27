#!/bin/bash

set -e

db=novichenkobot
db_rfc=postgres:///$db
stepsize=3

# create log directories
log=log/$(date +'%Y-%m-%d-%H-%M-%S')
mkdir -p $log
ln -sfn "$(pwd)/$log" log/newest

# create the database if it doesn't exist
#if [ ! -e "$db" ]; then
    #python3 -m NovichenkoBot.sqlalchemy_scheduler --db $db_rfc --add_seeds inputs/hostnames.csv --create_db
#fi

# loop through the hostnames in batches of $stepsize,
# starting a crawler dedicated to each batch
i=0
while true; do
    res=$(psql $db <<EOF 
    select hostname from seed_hostnames limit $stepsize offset $((i * $stepsize));
EOF
)
    hosts=$(echo "$res" | tail -n +3 | head -n -1)
    if [ "$hosts" = "" ] || [ $i = 50 ]; then
        break
        echo done
    fi
    restrictions=$(echo $hosts | tr ' ' ',')
    echo $restrictions
    nohup scrapy crawl general -s HOSTNAME_RESTRICTIONS=$restrictions -a db=$db_rfc > $log/general.$(printf "%04d" $i) 2>&1 &
    echo $! >> $log/pids
    i=$(( i + 1 ))
done
