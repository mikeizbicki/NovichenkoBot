/*
 * Rollup utility code taken from
 * https://www.citusdata.com/blog/2018/06/14/scalable-incremental-data-aggregation/
 *
 * The incremental_rollup_window function has been modified so that it doesn't
 * rollup the entire table at once, but in smaller chuncks;
 * this is useful for rollingup large tables incrementally that have already been created
 */

CREATE TABLE rollups (
    name text primary key,
    event_table_name text not null,
    event_id_sequence_name text not null,
    sql text not null,
    last_aggregated_id bigint default 0
);

CREATE FUNCTION incremental_rollup_window(
    rollup_name text, 
    max_rollup_size bigint default 100000,
    force_safe boolean default true,
    OUT window_start bigint,
    OUT window_end bigint
)
RETURNS record
LANGUAGE plpgsql
AS $function$
DECLARE
    table_to_lock regclass;
BEGIN
    /*
     * Perform aggregation from the last aggregated ID + 1 up to the last committed ID.
     * We do a SELECT .. FOR UPDATE on the row in the rollup table to prevent
     * aggregations from running concurrently.
     */
    SELECT event_table_name, last_aggregated_id+1, LEAST(last_aggregated_id+max_rollup_size+1,pg_sequence_last_value(event_id_sequence_name))
    INTO table_to_lock, window_start, window_end
    FROM rollups
    WHERE name = rollup_name FOR UPDATE;

    IF NOT FOUND THEN
        RAISE 'rollup ''%'' is not in the rollups table', rollup_name;
    END IF;

    IF window_end IS NULL THEN
        /* sequence was never used */
        window_end := 0;
        RETURN;
    END IF;

    /*
     * Play a little trick: We very briefly lock the table for writes in order to
     * wait for all pending writes to finish. That way, we are sure that there are
     * no more uncommitted writes with a identifier lower or equal to window_end.
     * By throwing an exception, we release the lock immediately after obtaining it
     * such that writes can resume.
     */
    IF force_safe THEN
        BEGIN
            -- NOTE: The line below is modified from the original to acquire
            -- a ROW EXCLUSIVE lock rather than an exclusive lock; this lock still
            -- prevents update/insert/delete operations on the table, but it does
            -- not block on autovacuum (SHARE UPDATE EXCLUSIVE lock) or
            -- create index (SHARE lock).  I believe everything is therefore still
            -- correct, but this is magic beyond my domain expertise, so I'm
            -- not 100% certain.
            EXECUTE format('LOCK %s IN ROW EXCLUSIVE MODE', table_to_lock);
            RAISE 'release table lock';
        EXCEPTION WHEN OTHERS THEN
        END;
    END IF;

    /*
     * Remember the end of the window to continue from there next time.
     */
    UPDATE rollups SET last_aggregated_id = window_end-1 WHERE name = rollup_name;
END;
$function$;

CREATE FUNCTION do_rollup(
    name text,
    max_rollup_size bigint default 100000000,
    force_safe boolean default true,
    OUT start_id bigint, 
    OUT end_id bigint
)
RETURNS record
LANGUAGE plpgsql
AS $function$
DECLARE
    sql_command text;
BEGIN
    /* determine which page views we can safely aggregate */
    SELECT window_start, window_end INTO start_id, end_id
    FROM incremental_rollup_window(name,max_rollup_size,force_safe);

    /* exit early if there are no new page views to aggregate */
    IF start_id > end_id THEN RETURN; END IF;

    /* this is the new code that gets the rollup command from the table
     * and executes it */
    SELECT rollups.sql 
    INTO sql_command
    FROM rollups
    WHERE rollups.name = do_rollup.name;

    EXECUTE sql_command USING start_id,end_id;
END;
$function$;

/*
 * Rollup table for frontier
 */

CREATE TABLE frontier_hostname (
    hostname TEXT PRIMARY KEY,
    num INTEGER NOT NULL,
    num_0 INTEGER NOT NULL,
    num_10 INTEGER NOT NULL,
    num_100 INTEGER NOT NULL,
    num_1000 INTEGER NOT NULL,
    num_10000 INTEGER NOT NULL,
    num_100000 INTEGER NOT NULL,
    num_1000000 INTEGER NOT NULL
);

INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('frontier_hostname', 'frontier', 'frontier_id_frontier_seq', $$
    INSERT INTO frontier_hostname 
        (hostname,num,num_0,num_10,num_100,num_1000,num_10000,num_100000,num_1000000)
    SELECT
        substring(reverse(hostname_reversed) from 2) as hostname,
        count(1),
        sum(CASE WHEN priority>0 THEN 1 ELSE 0 END),
        sum(CASE WHEN priority>10 THEN 1 ELSE 0 END),
        sum(CASE WHEN priority>100 THEN 1 ELSE 0 END),
        sum(CASE WHEN priority>1000 THEN 1 ELSE 0 END),
        sum(CASE WHEN priority>10000 THEN 1 ELSE 0 END),
        sum(CASE WHEN priority>100000 THEN 1 ELSE 0 END),
        sum(CASE WHEN priority>1000000 THEN 1 ELSE 0 END)
    FROM frontier
    WHERE
        frontier.id_frontier >= $1 AND 
        frontier.id_frontier < $2 
    GROUP BY hostname
    ON CONFLICT (hostname)
    DO UPDATE SET 
        num = frontier_hostname.num+excluded.num,
        num_0 = frontier_hostname.num_0+excluded.num_0,
        num_10 = frontier_hostname.num_10+excluded.num_10,
        num_100 = frontier_hostname.num_100+excluded.num_100,
        num_1000 = frontier_hostname.num_1000+excluded.num_1000,
        num_10000 = frontier_hostname.num_10000+excluded.num_10000,
        num_100000 = frontier_hostname.num_100000+excluded.num_100000,
        num_1000000 = frontier_hostname.num_1000000+excluded.num_1000000
    ;
$$);

/*
 * Rollup table for requests
 */

CREATE TABLE requests_hostname (
    hostname TEXT PRIMARY KEY,
    num INTEGER NOT NULL
);

INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('requests_hostname', 'requests', 'requests_id_requests_seq', $$
    INSERT INTO requests_hostname 
        (hostname,num)
    SELECT
        substring(reverse(hostname_reversed) from 2) as hostname,
        count(1)
    FROM requests 
    INNER JOIN frontier ON frontier.id_frontier=requests.id_frontier
    WHERE
        requests.id_requests >= $1 AND 
        requests.id_requests < $2 
    GROUP BY hostname
    ON CONFLICT (hostname)
    DO UPDATE SET 
        num = requests_hostname.num+excluded.num
    ;
$$);

/*
 * Rollup table for refs
 */

-- FIXME: delete table
CREATE TABLE refs_hostname (
    year SMALLINT,
    hostname_source TEXT,
    hostname_target TEXT,
    type TEXT,
    num BIGINT,
    PRIMARY KEY (year,hostname_source,hostname_target,type)
);

INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('refs_hostname', 'refs', 'refs_id_refs_seq', $$
    INSERT INTO refs_hostname
        (year,hostname_source,hostname_target,type,num)
    SELECT
        CASE 
            WHEN articles.pub_time IS NULL
            THEN -1
            ELSE extract(year from articles.pub_time) 
            END AS year_source,
        urls_source.hostname AS hostname_source,
        urls_target.hostname AS hostname_target,
        type,
        count(1) 
    FROM articles
    INNER JOIN refs ON articles.id_articles = refs.source
    INNER JOIN urls AS urls_source ON urls_source.id_urls = articles.id_urls
    INNER JOIN urls AS urls_target ON urls_target.id_urls = refs.target
    WHERE
        refs.id_refs < 1000
        --refs.id_refs >= $1 AND 
        --refs.id_refs < $2 
    GROUP BY year_source,urls_source.hostname,urls_target.hostname,type
    ON CONFLICT (year,hostname_source,hostname_target,type)
    DO UPDATE SET num = refs_hostname.num+excluded.num
    ;
$$);

-- FIXME: delete table
CREATE TABLE refs_keywords (
    year SMALLINT,
    hostname_source TEXT,
    hostname_target TEXT,
    type TEXT,
    num BIGINT,
    PRIMARY KEY (year,hostname_source,hostname_target,type)
);

INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('refs_keywords', 'keywords', 'keywords_id_keywords_seq', $$
    INSERT INTO refs_keywords
        (year,hostname_source,hostname_target,type,num)
    SELECT
        CASE 
            WHEN articles.pub_time IS NULL
            THEN -1
            ELSE extract(year from articles.pub_time) 
            END AS year_source,
        urls_source.hostname AS hostname_source,
        urls_target.hostname AS hostname_target,
        type,
        count(1) 
    --FROM refs 
    --INNER JOIN articles ON articles.id_articles = refs.source
    --INNER JOIN keywords on keywords.id_articles = articles.id_articles
    FROM keywords
    INNER JOIN articles ON articles.id_articles = keywords.id_articles
    INNER JOIN refs on refs.source = articles.id_articles
    INNER JOIN urls AS urls_source ON urls_source.id_urls = articles.id_urls
    INNER JOIN urls AS urls_target ON urls_target.id_urls = refs.target
    WHERE
        (keywords.num_text > 0 OR keywords.num_title > 0) AND
        --keywords.id_keywords < 1000
        keywords.id_keywords >= $1 AND 
        keywords.id_keywords < $2 
        --refs.id_refs < 1000
        --refs.id_refs >= $1 AND 
        --refs.id_refs < $2 
    GROUP BY year_source,urls_source.hostname,urls_target.hostname,type
    ON CONFLICT (year,hostname_source,hostname_target,type)
    DO UPDATE SET num = refs_keywords.num+excluded.num
    ;
$$);

CREATE TABLE refs_summary (
    year SMALLINT,
    hostname_source TEXT,
    hostname_target TEXT,
    type TEXT,
    num_all BIGINT,
    num_keywords BIGINT,
    distinct_all hll,
    distinct_keywords hll,
    PRIMARY KEY (year,hostname_source,hostname_target,type)
);
CREATE INDEX refs_summary_index_hostname_source on refs_summary(hostname_source);

INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('refs_summary', 'keywords', 'keywords_id_keywords_seq', $$
    INSERT INTO refs_summary
        (year,hostname_source,hostname_target,type,num_all,num_keywords,distinct_all,distinct_keywords)
    SELECT
        CASE 
            WHEN articles.pub_time IS NULL
            THEN -1
            ELSE extract(year from articles.pub_time) 
            END AS year_source,
        articles.hostname AS hostname_source,
        urls_target.hostname AS hostname_target,
        type,
        sum(1),
        sum(CASE
            WHEN keywords.num_text > 0 OR keywords.num_title > 0
            THEN 1
            ELSE 0
            END),
        hll_add_agg(hll_hash_bigint(CASE 
            WHEN articles.id_urls_canonical = 2425 
            THEN articles.id_urls 
            ELSE articles.id_urls_canonical 
            END)) as distinct_all,
        hll_union_agg(CASE
            WHEN keywords.num_text > 0 OR keywords.num_title > 0
            THEN hll_add(hll_empty(),(hll_hash_bigint(CASE 
                WHEN articles.id_urls_canonical = 2425 
                THEN articles.id_urls 
                ELSE articles.id_urls_canonical 
                END)))
            ELSE hll_empty()
            END) as distinct_keywords 
    FROM keywords
    INNER JOIN articles ON articles.id_articles = keywords.id_articles
    INNER JOIN refs ON refs.source = articles.id_articles
    INNER JOIN urls AS urls_target ON urls_target.id_urls = refs.target
    WHERE
        --keywords.id_keywords < 100000
        keywords.id_keywords >= $1 AND 
        keywords.id_keywords < $2 
    GROUP BY 1,2,3,4
    ON CONFLICT (year,hostname_source,hostname_target,type)
    DO UPDATE SET 
        num_all = refs_summary.num_all + excluded.num_all,
        num_keywords = refs_summary.num_keywords + excluded.num_keywords,
        distinct_all = refs_summary.distinct_all || excluded.distinct_all,
        distinct_keywords = refs_summary.distinct_keywords || excluded.distinct_keywords
    ;
$$);

SELECT 
    hostname_source,
    hostname_target,
    sum(num_all),
    sum(num_keywords),
    sum(#distinct_all) as distinct_all,
    sum(#distinct_keywords) as distinct_keywords
FROM refs_summary
WHERE hostname_source='breitbart.com'
GROUP BY hostname_source,hostname_target
ORDER BY distinct_keywords desc,distinct_all desc;

/*
SELECT 
    hostname_source,
    hostname_target,
    sum(#distinct_all) as distinct_all,
    sum(#distinct_keywords) as distinct_keywords
FROM refs_summary
WHERE hostname_source='www.foxnews.com'
GROUP BY hostname_source,hostname_target
ORDER BY distinct_keywords DESC,distinct_all DESC;

select count(1) from (
SELECT hostname_source,hostname_target,sum(num) as num
FROM refs_keywords
WHERE 
    type='link' and (
    hostname_source='www.peru21.pe'
    --hostname_source='www.armscontrolwonk.com' or
    --hostname_source='www.nknews.org' or
    --hostname_source='www.northkoreatech.org' or
    --hostname_source='www.thehill.org' or
    --hostname_source='www.breitbart.com' or
    --hostname_source='thediplomat.com' or
    --hostname_source='foreignpolicy.com'
    )
GROUP BY hostname_source,hostname_target
ORDER BY num DESC
)t;

select hostname_target,sum(num) as num
from refs_keywords
where 
    type='link' and 
    hostname_source in (select hostname from hostname_productivity limit 100) and
    hostname_target not in (select hostname from crawlable_hostnames) and
    right(hostname_target, length(hostname_target)-4) not in (SELECT hostname FROM crawlable_hostnames) and
    hostname_target not in (select hostname from responses_timestamp_hostname)
group by hostname_target
order by num desc;

SELECT DISTINCT hostname_target as hostname
FROM refs_keywords
WHERE 
    type='link' and (
    hostname_source='www.armscontrolwonk.com' or
    hostname_source='www.nknews.org' or
    hostname_source='www.northkoreatech.org' or
    hostname_source='www.thehill.org' or
    hostname_source='www.breitbart.com' or
    hostname_source='thediplomat.com' or
    hostname_source='foreignpolicy.com'
    )
    and not (
    hostname_target like '%facebook.%' or
    hostname_target like '%instagram.%' or
    hostname_target like '%scribd.%' or
    hostname_target like '%twitter.%' or
    hostname_target like '%reddit.%' or
    hostname_target like '%pinterest.%' or
    hostname_target like '%youtube.%' or
    hostname_target like '%youtu.be%' or
    hostname_target like '%google.%' or
    hostname_target like '%wikipedia.%' or
    hostname_target like '%wikimedia.%' or
    hostname_target like '%linkedin.%' or
    hostname_target like '%yahoo.%' or
    hostname_target like '%archive.%' or
    hostname_target like '%flickr.%' or
    hostname_target like '%answers.%' or
    hostname_target like '%imgur.%'
    )
;
*/

/*
 * Rollup table for urls 
 */

CREATE TABLE urls_summary (
    hostname TEXT NOT NULL,
    distinct_path hll NOT NULL,
    distinct_path_query hll NOT NULL,
    num BIGINT NOT NULL,
    PRIMARY KEY (hostname)
);

INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('urls_summary', 'urls', 'urls_id_urls_seq', $$
    INSERT INTO urls_summary
        (hostname,distinct_path,distinct_path_query,num)
    SELECT
        hostname,
        hll_add_agg(hll_hash_text(path)),
        hll_add_agg(hll_hash_text(path || query)),
        count(1)
    FROM urls 
    WHERE 
        id_urls>=$1 AND
        id_urls<$2
    GROUP BY hostname
    ON CONFLICT (hostname)
    DO UPDATE SET   
        distinct_path = urls_summary.distinct_path || excluded.distinct_path,
        distinct_path_query = urls_summary.distinct_path_query || excluded.distinct_path_query,
        num = urls_summary.num+excluded.num
    ;
$$);

/*
 * Rollup table for responses
 */

CREATE TABLE responses_timestamp_hostname (
    timestamp TIMESTAMP,
    hostname TEXT,
    num BIGINT NOT NULL,
    PRIMARY KEY (timestamp,hostname)
);
CREATE INDEX responses_timestamp_hostname_index ON responses_timestamp_hostname(hostname);

INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('responses_timestamp_hostname', 'responses', 'responses_id_responses_seq', $$
    INSERT INTO responses_timestamp_hostname
        (timestamp,hostname,num)
    SELECT
        date_trunc('hour',timestamp_received) as timestamp,
        hostname,
        count(1)
    FROM responses
    WHERE 
        id_responses>=$1 AND
        id_responses<$2
    GROUP BY timestamp,hostname
    ON CONFLICT (timestamp,hostname)
    DO UPDATE SET num = responses_timestamp_hostname.num+excluded.num
    ;
$$);

CREATE TABLE responses_summary (
    timestamp TIMESTAMP,
    bytes BIGINT NOT NULL,
    num BIGINT NOT NULL,
    num_http_200 BIGINT NOT NULL,
    num_http_3xx BIGINT NOT NULL,
    num_http_4xx BIGINT NOT NULL,
    num_twisted_fail BIGINT NOT NULL,
    PRIMARY KEY (timestamp)
);
INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('responses_summary', 'responses', 'responses_id_responses_seq', $$
    INSERT INTO responses_summary
        ( timestamp
        , bytes
        , num
        , num_http_200
        , num_http_3xx
        , num_http_4xx
        , num_twisted_fail
        )
    SELECT
        date_trunc('minute',timestamp_received) as timestamp,
        sum(CASE WHEN bytes is null THEN 0 ELSE bytes END) as bytes,
        count(1) as num,
        sum(CASE WHEN http_status = '200' THEN 1 ELSE 0 END) as num_http_200,
        sum(CASE WHEN http_status >= '300' and http_status < '400' THEN 1 ELSE 0 END) as num_http_3xx,
        sum(CASE WHEN http_status >= '400' and http_status < '500' THEN 1 ELSE 0 END) as num_http_4xx,
        sum(CASE WHEN twisted_status != 'Success' THEN 1 ELSE 0 END) as num_twisted_fail
    FROM responses
    WHERE 
        id_responses>=$1 AND
        id_responses<$2
    GROUP BY timestamp
    ON CONFLICT (timestamp)
    DO UPDATE SET 
        bytes = responses_summary.bytes+excluded.bytes,
        num = responses_summary.num+excluded.num,
        num_http_200 = responses_summary.num_http_200+excluded.num_http_200,
        num_http_3xx = responses_summary.num_http_3xx+excluded.num_http_3xx,
        num_http_4xx = responses_summary.num_http_4xx+excluded.num_http_4xx,
        num_twisted_fail = responses_summary.num_twisted_fail+excluded.num_twisted_fail
    ;
$$);

/*
 * rollup for articles
 */

-- FIXME: table much slower than articles_summary2 due to having to access the text,
-- which is many bytes; probably should delete it
CREATE TABLE articles_summary (
    day TIMESTAMP,
    hostname TEXT,
    keyword BOOL,
    num BIGINT NOT NULL,
    num_distinct_url hll NOT NULL,
    num_distinct_title hll NOT NULL,
    num_distinct_text hll NOT NULL, 
    PRIMARY KEY (day,hostname,keyword)
);

INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('articles_summary', 'keywords', 'keywords_id_keywords_seq', $$
    INSERT INTO articles_summary
        ( day
        , hostname
        , keyword
        , num
        , num_distinct_url
        , num_distinct_title
        , num_distinct_text
        )
    SELECT
        CASE WHEN pub_time is NULL THEN '-infinity' ELSE date_trunc('day',pub_time) END as day,
        hostname,
        CASE WHEN num_title>0 or num_text>0 THEN true ELSE false END as keyword,
        count(1) as num,
        /* this case expression is id_urls_canonical_ from get_valid_articles() */
        hll_add_agg(hll_hash_bigint(CASE 
            WHEN articles.id_urls_canonical = 2425 
            THEN articles.id_urls 
            ELSE articles.id_urls_canonical 
            END)) as num_distinct_url,
        hll_add_agg(hll_hash_text(title)) as num_distinct_title,
        hll_add_agg(hll_hash_text(text)) as num_distinct_text
    FROM keywords
    INNER JOIN articles on articles.id_articles = keywords.id_articles
    WHERE 
        id_keywords>=$1 AND
        id_keywords<$2
    GROUP BY 1,2,3 
    ON CONFLICT (day,hostname,keyword)
    DO UPDATE SET 
        num = articles_summary.num + excluded.num,
        num_distinct_url = articles_summary.num_distinct_url || excluded.num_distinct_url,
        num_distinct_title = articles_summary.num_distinct_title || excluded.num_distinct_title,
        num_distinct_text = articles_summary.num_distinct_text || excluded.num_distinct_text
    ;
$$);

CREATE TABLE articles_lang (
    hostname TEXT,
    lang VARCHAR(2),
    num_distinct hll NOT NULL,
    PRIMARY KEY(hostname,lang)
);
INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('articles_lang', 'articles', 'articles_id_articles_seq', $$
    INSERT INTO articles_lang
        ( hostname
        , lang
        , num_distinct
        )
    SELECT
        hostname,
        lang,
        /* this case expression is id_urls_canonical_ from get_valid_articles() */
        hll_add_agg(hll_hash_bigint(CASE 
            WHEN articles.id_urls_canonical = 2425 
            THEN articles.id_urls 
            ELSE articles.id_urls_canonical 
            END)) as num_distinct
    FROM articles
    WHERE 
        id_articles>=$1 AND
        id_articles<$2
    GROUP BY 1,2 
    ON CONFLICT (hostname,lang)
    DO UPDATE SET 
        num_distinct = articles_lang.num_distinct || excluded.num_distinct
    ;
$$);

SELECT 
    lang,
    num_distinct,
    num_distinct/sum(num_distinct) over () as fraction
FROM (
    SELECT 
        lang,
        sum(#num_distinct) as num_distinct
    FROM articles_lang
    GROUP BY lang
) t1
ORDER BY num_distinct DESC;

CREATE TABLE articles_summary2 (
    day TIMESTAMP,
    hostname TEXT,
    keyword BOOL,
    num BIGINT NOT NULL,
    num_distinct hll NOT NULL,
    PRIMARY KEY (day,hostname,keyword)
);

INSERT INTO rollups (name, event_table_name, event_id_sequence_name, sql)
VALUES ('articles_summary2', 'keywords', 'keywords_id_keywords_seq', $$
    INSERT INTO articles_summary2
        ( day
        , hostname
        , keyword
        , num
        , num_distinct
        )
    SELECT
        CASE WHEN pub_time is NULL THEN '-infinity' ELSE date_trunc('day',pub_time) END as day,
        hostname,
        CASE WHEN num_title>0 or num_text>0 THEN true ELSE false END as keyword,
        count(1) as num,
        /* this case expression is id_urls_canonical_ from get_valid_articles() */
        hll_add_agg(hll_hash_bigint(CASE 
            WHEN articles.id_urls_canonical = 2425 
            THEN articles.id_urls 
            ELSE articles.id_urls_canonical 
            END)) as num_distinct
    FROM keywords
    INNER JOIN articles on articles.id_articles = keywords.id_articles
    WHERE 
        id_keywords>=$1 AND
        id_keywords<$2
    GROUP BY 1,2,3 
    ON CONFLICT (day,hostname,keyword)
    DO UPDATE SET 
        num = articles_summary2.num + excluded.num,
        num_distinct = articles_summary2.num_distinct || excluded.num_distinct
    ;
$$);

/*
SELECT 
    hostname,
    --date_trunc('year',day) as year,
    #hll_union_agg(num_distinct) as num_distinct
FROM articles_summary2
WHERE 
    keyword=true AND 
    day!='-infinity'
GROUP BY hostname --,year
ORDER BY num_distinct DESC;

SELECT 
    t1.hostname,
    t1.year,
    num_distinct,
    CASE WHEN num_distinct_keyword IS NULL THEN 0 ELSE num_distinct_keyword END,
    CASE WHEN num_distinct_keyword IS NULL THEN 0 ELSE num_distinct_keyword END / num_distinct as keyword_fraction
FROM (
    SELECT 
        hostname,
        extract(year from day) as year,
        sum(#num_distinct) num_distinct
    FROM articles_summary2
    WHERE hostname='www.nytimes.com'
    GROUP BY hostname,year
) AS t1
LEFT JOIN (
    SELECT 
        hostname,
        extract(year from day) as year,
        sum(#num_distinct) num_distinct_keyword
    FROM articles_summary2
    WHERE hostname='www.nytimes.com' AND keyword=true
    GROUP BY hostname,year
) AS t2 on t1.hostname = t2.hostname and t1.year = t2.year
ORDER BY year DESC;

SELECT 
    hostname,
    extract(year from day) as year,
    sum(#num_distinct_url) distinct_url,
    sum(#num_distinct_title) distinct_title,
    sum(#num_distinct_text) distinct_text
FROM articles_summary
WHERE hostname='www.nytimes.com'
GROUP BY hostname,year
ORDER BY year DESC;
*/

