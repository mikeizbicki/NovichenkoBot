#!/bin/sh

topic=dprk_kimdying
subtopic=
freshness=2020-05-01..2020-05-03

LANGS='ar de en es fa fr he id it ja ko pt ru sv tr uk ur vi zh-Hans zh-Hant'

set -e

# translate queries
for lang in $LANGS; do
    subtopic_str=''
    if [ ! -e $subtopic ]; then
        subtopic_str="/$subtopic"
    fi
    python3 -u Novichenko/Seed/translate_words.py --input=db/queries/$topic${subtopic_str}.en --target_lang=$lang
done

# search for seeds
mkdir -p db/seeds/${topic}_search/
for lang in $LANGS; do
    echo ==================================
    date
    echo $lang
    echo ==================================

    freshness_arg=''
    if [ ! -e $freshness ]; then
        freshness_arg="--freshness=$freshness"
    fi
    input_arg="--query_file=db/queries/$topic.$lang"
    if [ ! -r $subtopic ]; then
        input_arg="--prefix_file=db/queries/$topic.$lang --query_file=db/queries/$topic/$subtopic.$lang"
    fi

    python3 -u Novichenko/Seed/create_seeds_from_search.py \
        --search_type=deep \
        --count=400 \
        $freshness_arg \
        $input_arg \
        --output_file=db/seeds/${topic}_search/$subtopic.$freshness.$lang \
        --lang=$lang
done

# insert seeds
for f in db/seeds/${topic}_search/$subtopic.$freshness.${lang}*; do
    python3 -u Novichenko/Seed/insert_seeds.py --inputs=$f --hostnames_priority=$topic.$subtopic.$freshness > nohup/nohup.insert_seeds.$(basename $f) &
done
