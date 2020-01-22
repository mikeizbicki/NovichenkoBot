#!/bin/sh

set -e

########################################
# create log directories
########################################

log=log/labelers_$(date +'%Y-%m-%d-%H-%M-%S')
mkdir -p $log
ln -sfn "$(pwd)/$log" log/labelers

########################################
# run labelrs
########################################

hostnames='
www.nti.org
www.npr.org
www.lawfareblog.com
www.foxnews.com
borgenproject.org
warontherocks.com
juche007-anglo-peopleskoreafriendship.blogspot.com
kfausa.org
'
#nationalinterest.org
#www.usatoday.com
#npolicy.org
#armscontrol.org
#www.schneier.com
#freedomhouse.org
#www.tomdispatch.com
#freekorea.us
#'
#www.armscontrolwonk.com 
##www.reuters.com 
#www.nknews.org 
#foreignpolicy.com 
#www.nytimes.com 
#apnews.com 
#www.washingtonpost.com
#www.usatoday.com
#'
#www.northkoreatech.org 
#www.janes.com 
#sinonk.com 
#'
CUDA_DEVICE=0
for hostname in $(echo $hostnames); do
#for hostname in www.janes.com; do
    nohup python3 -u scripts/label_hostname.py --articles_per_iteration=100 --hostname=$hostname --CUDA_VISIBLE_DEVICES=$CUDA_DEVICE > $log/label-$hostname &
    echo $! >> $log/pids
    CUDA_DEVICE=$(expr 1 + $CUDA_DEVICE)
done
