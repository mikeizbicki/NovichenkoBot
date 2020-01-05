#!/bin/sh

set -e

views="
hostnames_articles
lang_stats
hostnames_responses
hostname_productivity
articles_per_year
"

for view in $views; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') $view"
    psql novichenkobot -c "refresh materialized view $view;"
done
