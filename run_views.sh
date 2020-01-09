#!/bin/sh

set -e

views="
lang_stats
articles_lang
articles_per_year
hostnames_articles
hostnames_responses
hostname_productivity
hostname_progress
refs_summary_simple
"

for view in $views; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') view=$view"
    psql novichenkobot -c "refresh materialized view $view;" &
done

