CREATE MATERIALIZED VIEW hostnames_articles AS
SELECT DISTINCT hostname FROM articles_lang;

CREATE MATERIALIZED VIEW lang_stats AS
SELECT
    lang,
    sum(#num_distinct) as num_distinct
FROM articles_lang
GROUP BY lang;

CREATE MATERIALIZED VIEW hostnames_responses AS
SELECT DISTINCT hostname FROM responses_timestamp_hostname;

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
RIGHT JOIN (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_total
    FROM articles_summary2
    WHERE 
        day!='-infinity'
    GROUP BY hostname 
) t2v ON t1v.hostname = t2v.hostname
RIGHT JOIN (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_keywords
    FROM articles_summary2
    WHERE 
        keyword=true 
    GROUP BY hostname 
) t1 ON t1v.hostname = t1.hostname
RIGHT JOIN (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_total
    FROM articles_summary2
    GROUP BY hostname 
) t2 ON t1v.hostname = t2.hostname
ORDER BY priority DESC;


/*
CREATE VIEW articles_per_year AS 
    SELECT 
        hostname,
        extract(YEAR FROM pub_time) as year,
        count(1) as num
    FROM articles
    GROUP BY hostname,year
    ORDER BY hostname,year;
*/

CREATE MATERIALIZED VIEW articles_per_year AS
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
    GROUP BY hostname,year
) AS t2 on t1.hostname = t2.hostname and t1.year = t2.year
ORDER BY t1.hostname DESC,year DESC;

/*
SELECT hostname,num_distinct_keywords,num_distinct_total,keyword_fraction
FROM hostname_productivity
WHERE
    hostname not in (SELECT hostname FROM crawlable_hostnames) AND
    right(hostname, length(hostname)-4) not in (SELECT hostname FROM crawlable_hostnames)
    --AND hostname like '%.gov'
    ;

select hostname_target from (
SELECT 
    hostname_target
    --hostname_target,
    --sum(#distinct_keywords) as distinct_keywords
FROM refs_summary
WHERE 
    type='link' and (
        --hostname_source='www.peru21.pe' or
        --hostname_source='www.armscontrolwonk.com' or
        --hostname_source='www.nknews.org' or
        --hostname_source='www.northkoreatech.org' or
        --hostname_source='www.thehill.org' or
        --hostname_source='thediplomat.com' or
        --hostname_source='foreignpolicy.com'
    --)
    --and not (
    --hostname_target like '%facebook.%' or
    --hostname_target like '%instagram.%' or
    --hostname_target like '%scribd.%' or
    --hostname_target like '%twitter.%' or
    --hostname_target like '%reddit.%' or
    --hostname_target like '%pinterest.%' or
    --hostname_target like '%youtube.%' or
    --hostname_target like '%youtu.be%' or
    --hostname_target like '%google.%' or
    --hostname_target like '%wikipedia.%' or
    --hostname_target like '%wikimedia.%' or
    --hostname_target like '%linkedin.%' or
    --hostname_target like '%yahoo.%' or
    --hostname_target like '%archive.%' or
    --hostname_target like '%flickr.%' or
    --hostname_target like '%answers.%' or
    --hostname_target like '%imgur.%'
    --)
GROUP BY hostname_target
ORDER BY distinct_keywords desc
)t
WHERE distinct_keywords>4;
*/
