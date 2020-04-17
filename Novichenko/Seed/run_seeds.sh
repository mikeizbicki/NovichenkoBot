#!/bin/sh

for lang in ko ja zh-CN ru es ar de fa fr he id it ms ps sv vi; do 
    echo ==================================
    date
    echo $lang
    echo ==================================
    python3 -u NovichenkoSearch/create_seeds_from_search.py --input_dir=. --lang=$lang
done
