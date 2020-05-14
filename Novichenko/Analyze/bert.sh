#!/bin/sh

set -e

logdir=log/bert
mkdir -p $logdir

bert_prefix="nohup nice -n -1 nohup python3 -u Novichenko/Analyze/bert.py "

CUDA_VISIBLE_DEVICES=0 $bert_prefix >> $logdir/bert.0 --source=twitter.tweets.text &
CUDA_VISIBLE_DEVICES=1 $bert_prefix >> $logdir/bert.1 --source=twitter.tweets.text --id0=1233395535912144897 &

CUDA_VISIBLE_DEVICES=2 $bert_prefix >> $logdir/bert.2 --source=public.articles.title --id0=200000000 &
CUDA_VISIBLE_DEVICES=3 $bert_prefix >> $logdir/bert.3 --source=public.articles.title --id0=220000000 &

CUDA_VISIBLE_DEVICES=4 $bert_prefix >> $logdir/bert.4 --source=public.articles.text --id0=200000000 --texts_per_iteration=128 --max_length=512 &
CUDA_VISIBLE_DEVICES=5 $bert_prefix >> $logdir/bert.5 --source=public.articles.text --id0=220000000 --texts_per_iteration=128 --max_length=512 &
CUDA_VISIBLE_DEVICES=6 $bert_prefix >> $logdir/bert.6 --source=public.articles.text --id0=210000000 --texts_per_iteration=128 --max_length=512 &
CUDA_VISIBLE_DEVICES=7 $bert_prefix >> $logdir/bert.7 --source=public.articles.text --id0=230000000 --texts_per_iteration=128 --max_length=512 &
