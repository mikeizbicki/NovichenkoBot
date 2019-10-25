#!/bin/bash

set -e

db=/home/mizbicki/scrapedata.db
db_rfc=sqlite:///$db
stepsize=3

# create log directories
log=log/$(date +'%Y-%m-%d-%H-%M-%S')
mkdir -p $log
ln -sfn "$(pwd)/$log" log/newest

# create the database if it doesn't exist
if [ ! -e "$db" ]; then
    python3 -m NovichenkoBot.sqlalchemy_scheduler --db $db_rfc --add_seeds inputs/hostnames.csv --create_db
fi

# loop through the hostnames in batches of $stepsize,
# starting a crawler dedicated to each batch
i=0
while true; do
    res=$(sqlite3 $db <<EOF 
    select hostname from seed_hostnames limit $((i * $stepsize)),$stepsize;
EOF
)
    if [ "$res" = "" ]; then
        break
        echo done
    fi
    restrictions=$(echo $res | tr ' ' ',')
    echo $restrictions
    nohup scrapy crawl general -s HOSTNAME_RESTRICTIONS=$restrictions -a db=$db_rfc > $log/general.$(printf "%04d" $i) 2>&1 &
    echo $! >> $log/pids
    i=$(( i + 1 ))
done
