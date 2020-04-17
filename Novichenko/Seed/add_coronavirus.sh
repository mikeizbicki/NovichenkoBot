#!/bin/sh

set -e

freshness=2020-04-13..2020-04-14

LANGS='ar de en es fa fr he id it ja ko pt ru sv tr uk ur vi zh-Hans zh-Hant'

for lang in $LANGS; do
    echo ==================================
    date
    echo $lang
    echo ==================================
    python3 -u NovichenkoSeed/create_seeds_from_search.py \
        --search_type=deep \
        --count=400 \
        --query_file=NovichenkoSeed/queries/coronavirus.$lang \
        --output_file=NovichenkoSeed/seeds/coronavirus_search4/coronavirus.$freshness.$lang \
        --freshness=$freshness \
        --lang=$lang
done

for f in NovichenkoSeed/seeds/coronavirus_search4/coronavirus.${freshness}*; do
    nohup python3 -u NovichenkoSeed/insert_seeds.py --inputs=$f --crawlable_hostnames_priority=coronavirus10 > nohup/nohup.insert_seeds.$(basename $f) &
done
