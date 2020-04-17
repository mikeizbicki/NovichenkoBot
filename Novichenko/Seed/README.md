# NovichenkoSeed

This directory contains files responsible for seeding the NovichenkoBot.

## Generating seeds from searches

<!--
```
LANGS0='ko ja zh-CN ru es'
LANGS1='ar de fa fr he id it ms ps sv vi'
LANGS2='mn ps ku pa ur hi bn pt no nn nb da se tl yi tr la sw gl'
LANGS="$LANGS1 $LANGS2"
```
-->

First, you must define a variable `LANGS` that contains all the languages you want to work with.
These are all the languages supported by bing translate:

```
LANGS=af ar bg bn bs ca cs cy da de el en es et fa fi fil fj fr ga he hi hr ht hu id is it ja kn ko lt lv mg mi ml ms mt mww nb nl otq pa pl pt pt-pt ro ru sk sl sm sr-Cyrl sr-Latn sv sw ta te th tlh-Latn tlh-Piqd to tr ty uk ur vi yua yue zh-Hans zh-Hant
```

And this is a subset of languages I have found convenient to work with:

```
LANGS='ar de en es fa fr he id it ja ko pt ru sv tr uk ur vi zh-Hans zh-Hant'
```

Next, we will create some seeds for doing a wide search.
Create a file called `queris/${term}.en` that contains one search term per line.
Translate this file into many languages using the command

```
for lang in $LANGS; do
    python3 -u Novichenko/Seed/translate_words.py --input=Novichenko/Seed/queries/dprk.en --target_lang=$lang
done
```

A wide search searches each TLD for each search term defined above.
It can be performed with the command
```
```

A deep search does a more detailed search on many more keywords.
Create a folder `queries/${term}`,
and inside that folder create a file `${topic}.terms.en` containing all the English search terms.
Translate the terms into the other languages with the command

```
for lang in $LANGS; do
    python3 -u Novichenko/Seed/translate_words.py --input=Novichenko/Seed/queries/dprk/smolinsky.en --target_lang=$lang
done
```

Create a folder `seeds/${term}_search`.
Generate the seeds for each term by running the command

```
for lang in $LANGS; do
    echo ==================================
    date
    echo $lang
    echo ==================================
    python3 -u Novichenko/Seed/create_seeds_from_search.py \
        --search_type=deep \
        --count=400 \
        --query_file=Novichenko/Seed/queries/coronavirus.$lang \
        --output_file=Novichenko/Seed/seeds/coronavirus_search4/coronavirus.2020-04-04,08.$lang \
        --freshness=2020-04-04..2020-04-08 \
        --lang=$lang
done
#for lang in $LANGS; do
#    echo ==================================
#    date
#    echo $lang
#    echo ==================================
#    python3 -u Novichenko/Seed/create_seeds_from_search.py \
#        --search_type=deep \
#        --count=200 \
#        --prefix_file=Novichenko/Seed/queries/dprk.$lang \
#        --query_file=Novichenko/Seed/queries/dprk/smolinsky.$lang \
#        --output_file=Novichenko/Seed/seeds/dprk_search/smolinsky.$lang \
#        --lang=$lang
#done
```

Finally, insert the seeds into the database

```
for f in Novichenko/Seed/seeds/coronavirus_search4/coronavirus.2020-04-04*; do
    nohup python3 -u Novichenko/Seed/insert_seeds.py --inputs=$f --crawlable_hostnames_priority=coronavirus10 > nohup/nohup.insert_seeds.$(basename $f) &
done
```

NOTES:

```
for lang in ko ja zh-CN ru es ar de fa fr he id it ms ps sv vi; do 
    echo ==================================
    date
    echo $lang
    echo ==================================
    python3 -u NovichenkoSearch/create_seeds_from_search.py --input_dir=. --lang=$lang
done
```

