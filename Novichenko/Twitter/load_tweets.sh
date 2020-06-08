#!/bin/sh

logdir=log/load_tweets
mkdir -p $logdir

#dates='20-02-0 20-02-1 20-02-2 20-02-3 20-02-0 20-02-1 20-02-2 20-02-3 20-03-0 20-03-1 20-03-2 20-03-3 20-04-0'
#
#for date in $dates; do
    #nohup python3 -u Novichenko/Twitter/load_tweets.py --inputs /data/Twitter\ dataset/geoTwitter${date}*.zip > $logdir/$date &
#done

#for file in ~/gdrive_twitter/geoTwitter20*.zip; do
for file in /data/tweets/geoTwitter20-03-18.zip; do
    nohup python3 -u Novichenko/Twitter/load_tweets.py --inputs $file > $logdir/$(basename $file) &
done
#nohup python3 -u Novichenko/Twitter/load_tweets.py --inputs /data/Twitter\ dataset/geoTwitter20-03-*.zip > $logdir/20- &
