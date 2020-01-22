/*
 * Views on articles_lang
 */
CREATE MATERIALIZED VIEW articles_lang_hostnames AS
SELECT DISTINCT hostname FROM articles_lang;
CREATE UNIQUE INDEX articles_lang_hostnames_index ON articles_lang_hostnames(hostname);

CREATE MATERIALIZED VIEW articles_lang_stats AS
SELECT
    lang,
    sum(#num_distinct) as num_distinct
FROM articles_lang
GROUP BY lang;
CREATE UNIQUE INDEX articles_lang_stats_index ON articles_lang_stats(lang);

/*
 * Views on responses_timestamp_hostname
 */

CREATE MATERIALIZED VIEW responses_timestamp_hostname_hostnames AS
SELECT DISTINCT hostname FROM responses_timestamp_hostname;
CREATE UNIQUE INDEX responses_timestamp_hostname_hostnames_index ON responses_timestamp_hostname_hostnames(hostname);

CREATE MATERIALIZED VIEW responses_timestamp_hostname_recent AS
SELECT hostname,sum(num) AS num 
FROM responses_timestamp_hostname 
WHERE timestamp > now() - interval '1 day' 
GROUP BY hostname 
ORDER BY num DESC;
CREATE UNIQUE INDEX responses_timestamp_hostname_recent_index ON responses_timestamp_hostname_recent(hostname);

/*
 * Views on refs_summary
 */

CREATE MATERIALIZED VIEW refs_summary_simple AS
SELECT
    hostname_source,
    hostname_target,
    sum(num_all) :: int as num_all,
    sum(num_keywords) :: int as num_keywords,
    (#hll_union_agg(distinct_all)) :: int as distinct_all,
    (#hll_union_agg(distinct_keywords)) :: int as distinct_keywords
FROM refs_summary
WHERE
    type='link'
GROUP BY hostname_source,hostname_target
ORDER BY hostname_source,hostname_target;
CREATE UNIQUE INDEX refs_summary_simple_index ON refs_summary_simple(hostname_source,hostname_target);

/*
 * Views on articles_summary2
 */
CREATE MATERIALIZED VIEW hostname_progress AS
SELECT
    frontier_hostname.hostname,
    COALESCE(frontier_hostname.num,0) as num_frontier,
    COALESCE(requests_hostname.num,0) as num_requests,
    COALESCE(ROUND(requests_hostname.num/frontier_hostname.num::numeric,4),0) as fraction_requested,
    COALESCE(responses_hostname.num,0) as num_responses,
    COALESCE(ROUND(responses_hostname.num/requests_hostname.num::numeric,4),0) as fraction_responded,
    COALESCE(articles_hostname.num,0) as num_articles,
    COALESCE(ROUND(articles_hostname.num/responses_hostname.num::numeric,4),0) as fraction_articles,
    COALESCE(articles_hostname_keyword.num,0) as num_articles_keyword,
    COALESCE(ROUND(articles_hostname_keyword.num/articles_hostname.num::numeric,4),0) as fraction_keyword
FROM frontier_hostname
LEFT OUTER JOIN requests_hostname ON requests_hostname.hostname=frontier_hostname.hostname
LEFT OUTER JOIN (
    SELECT hostname,sum(num) as num
    FROM responses_hostname
    GROUP BY hostname
) responses_hostname ON frontier_hostname.hostname=responses_hostname.hostname
LEFT OUTER JOIN (
    SELECT hostname,sum(num) as num
    FROM articles_summary2 
    WHERE keyword=FALSE 
    GROUP BY hostname
) articles_hostname ON frontier_hostname.hostname=articles_hostname.hostname
LEFT OUTER JOIN (
    SELECT hostname,sum(num) as num
    FROM articles_summary2 
    WHERE keyword=TRUE 
    GROUP BY hostname
) articles_hostname_keyword ON frontier_hostname.hostname=articles_hostname_keyword.hostname
;
CREATE UNIQUE INDEX hostname_progress_index ON hostname_progress(hostname);
CREATE INDEX hostname_progress_index2 ON hostname_progress(fraction_requested);

CREATE MATERIALIZED VIEW hostname_productivity AS
SELECT
    t2.hostname,
    COALESCE(t1v.num_distinct_keywords::int,0) AS valid_keywords,
    COALESCE(t2v.num_distinct_total::int,0) as valid_total,
    COALESCE(ROUND((t1v.num_distinct_keywords/t2v.num_distinct_total)::numeric,4),0) as valid_keyword_fraction,
    COALESCE(t1.num_distinct_keywords::int,0) AS all_keywords,
    COALESCE(t2.num_distinct_total::int,0) as all_total,
    COALESCE(ROUND((t1.num_distinct_keywords/t2.num_distinct_total)::numeric,4),0) as all_keyword_fraction,
    COALESCE(ROUND((t2v.num_distinct_total/t2.num_distinct_total)::numeric,4),0) as valid_fraction,
    COALESCE(ROUND((t1v.num_distinct_keywords*(t1v.num_distinct_keywords/t2v.num_distinct_total))::numeric,4),0) as score,
    COALESCE(ROUND((t1.num_distinct_keywords/t2.num_distinct_total/sqrt(t2.num_distinct_total) )::numeric,4),0) as priority
FROM (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_keywords
    FROM articles_summary2
    WHERE 
        keyword=true 
        AND day!='-infinity'
    GROUP BY hostname 
) t1v
FULL OUTER JOIN (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_total
    FROM articles_summary2
    WHERE 
        day!='-infinity'
    GROUP BY hostname 
) t2v ON t1v.hostname = t2v.hostname
FULL OUTER JOIN (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_keywords
    FROM articles_summary2
    WHERE 
        keyword=true 
    GROUP BY hostname 
) t1 ON t1v.hostname = t1.hostname
FULL OUTER JOIN (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_total
    FROM articles_summary2
    GROUP BY hostname 
) t2 ON t1v.hostname = t2.hostname
ORDER BY priority DESC;
CREATE UNIQUE INDEX hostname_productivity_index ON hostname_productivity(hostname);

CREATE MATERIALIZED VIEW hostname_peryear AS
SELECT
    t1.hostname,
    CASE WHEN t1.year = '-inf' THEN 'undefined' ELSE to_char(t1.year,'0000') END as year,
    num :: int,
    num_distinct :: int,
    COALESCE(num_distinct_keyword,0)::int as num_distinct_keyword,
    round((COALESCE(num_distinct_keyword,0) / num_distinct) :: numeric,4) as keyword_fraction
FROM (
    SELECT
        hostname,
        extract(year from day) as year,
        sum(num) as num,
        sum(#num_distinct) num_distinct
    FROM articles_summary2
    GROUP BY hostname,year
) AS t1
LEFT JOIN (
    SELECT
        hostname,
        extract(year from day) as year,
        sum(#num_distinct) num_distinct_keyword
    FROM articles_summary2
    WHERE keyword
    GROUP BY hostname,year
) AS t2 on t1.hostname = t2.hostname and t1.year = t2.year
ORDER BY t1.hostname DESC,year DESC;
CREATE UNIQUE INDEX hostname_peryear_index ON hostname_peryear(hostname,year);
