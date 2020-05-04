-- FIXME: This is the beginnings of a query for finding responses that failed to parse and therefore have no article;
-- we may need an index on articles.id_responses
SELECT
    id_responses
FROM responses
LEFT OUTER JOIN articles ON articles.id_responses = responses.id_responses
WHERE articles.id_responses IS NULL;

/*
 * This query returns the indexes ordered by how often they are scanned
 */

SELECT s.schemaname,
       s.relname AS tablename,
       s.indexrelname AS indexname,
       pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
       s.idx_scan,
       j.tablespace
FROM pg_catalog.pg_stat_user_indexes s
JOIN pg_catalog.pg_indexes j ON s.indexrelname = j.indexname
JOIN pg_catalog.pg_index i ON s.indexrelid = i.indexrelid
--WHERE 0 <>ALL (i.indkey)  -- no index column is an expression
  --AND NOT i.indisunique   -- is not a UNIQUE index
  --AND NOT EXISTS          -- does not enforce a constraint
         --(SELECT 1 FROM pg_catalog.pg_constraint c
          --WHERE c.conindid = s.indexrelid)
ORDER BY s.idx_scan DESC;

/*
 * This query returns the most expensive queries
 */

SELECT 
    query, 
    make_interval(secs => total_time/1000) as total_time,
    calls, 
    make_interval(secs => total_time/calls/1000) as time_per_call, 
    rows,
    100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
FROM pg_stat_statements 
ORDER BY total_time 
DESC LIMIT 10;

/*
 * The total time used by postgres queries
 * This should reset whenever run_crawlers.sh is run
 */

SELECT
    make_interval(secs => sum(total_time)/1000) as total_time
FROM pg_stat_statements;

/*
 * Calculate the disk usage of each table
 */
SELECT
    table_name,
    pg_size_pretty(table_size) AS table_size,
    pg_size_pretty(indexes_size) AS indexes_size,
    pg_size_pretty(total_size) AS total_size
FROM (
    SELECT
        table_name,
        pg_table_size(table_name) AS table_size,
        pg_indexes_size(table_name) AS indexes_size,
        pg_total_relation_size(table_name) AS total_size
    FROM (
        SELECT ('"' || table_schema || '"."' || table_name || '"') AS table_name
        FROM information_schema.tables
    ) AS all_tables
    ORDER BY total_size DESC
) AS pretty_sizes;

/*
 * Calculate the disk usage of each index
 */

SELECT
    indexname,
    pg_size_pretty(rel_size) as size,
    tablespace
FROM (
    SELECT 
        indexname,
        pg_relation_size(indexname::text) as rel_size,
        tablespace
    FROM pg_indexes
    WHERE 
        schemaname = 'public'
        -- AND tablespace = 'fastdata'
    ORDER BY rel_size DESC
)t;

-- total usage of fastdata indexes

SELECT
    pg_size_pretty(sum(rel_size)) as fastdata_size
FROM (
    SELECT 
        indexname,
        pg_relation_size(indexname::text) as rel_size
    FROM pg_indexes
    WHERE 
        schemaname = 'public'
        AND tablespace = 'fastdata'
    ORDER BY rel_size DESC
)t;

/*
 * list all the queries that are blocked and the reasons they are blocked
 */
SELECT
  COALESCE(blockingl.relation::regclass::text,blockingl.locktype) as locked_item,
  now() - blockeda.query_start AS waiting_duration, blockeda.pid AS blocked_pid,
  blockeda.query as blocked_query, blockedl.mode as blocked_mode,
  blockinga.pid AS blocking_pid, blockinga.query as blocking_query,
  blockingl.mode as blocking_mode
FROM pg_catalog.pg_locks blockedl
JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
JOIN pg_catalog.pg_locks blockingl ON(
  ( (blockingl.transactionid=blockedl.transactionid) OR
  (blockingl.relation=blockedl.relation AND blockingl.locktype=blockedl.locktype)
  ) AND blockedl.pid != blockingl.pid)
JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid
  AND blockinga.datid = blockeda.datid
WHERE NOT blockedl.granted
AND blockinga.datname = current_database();

-- cancel all blocked queries
SELECT pg_cancel_backend(blocked_pid)
FROM (
    SELECT
      COALESCE(blockingl.relation::regclass::text,blockingl.locktype) as locked_item,
      now() - blockeda.query_start AS waiting_duration, blockeda.pid AS blocked_pid,
      blockeda.query as blocked_query, blockedl.mode as blocked_mode,
      blockinga.pid AS blocking_pid, blockinga.query as blocking_query,
      blockingl.mode as blocking_mode
    FROM pg_catalog.pg_locks blockedl
    JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
    JOIN pg_catalog.pg_locks blockingl ON(
      ( (blockingl.transactionid=blockedl.transactionid) OR
      (blockingl.relation=blockedl.relation AND blockingl.locktype=blockedl.locktype)
      ) AND blockedl.pid != blockingl.pid)
    JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid
      AND blockinga.datid = blockeda.datid
    WHERE NOT blockedl.granted
    AND blockinga.datname = current_database()
)t;

/*
 * This query finds long running queries;
 * autovacuum often takes a long time and causes the rollup queries to stall
 *
 */
SELECT
  pid,
  now() - pg_stat_activity.query_start AS duration,
  query,
  state
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';

/*
 * calculate the diskspace used by a column in 
 * from: https://stackoverflow.com/questions/18316893/how-to-estimate-the-size-of-one-column-in-a-postgres-table

select
    pg_size_pretty(sum(pg_column_size(alltext))) as total_size,
    pg_size_pretty(avg(pg_column_size(alltext))) as average_size,
    sum(pg_column_size(alltext)) * 100.0 / pg_total_relation_size('articles') as percentage
from articles;
 */
 

/*
 * this query reinserts processed requests into the frontier so that they get crawled again
 *
 * this query has been run on: 
 * www.armscontrolwonk.com
 * thediplomat.com
 *
INSERT INTO frontier (id_urls, priority, timestamp_received, hostname_reversed)
SELECT DISTINCT ON (id_urls)
    id_urls,
    priority,
    current_timestamp,
    hostname_reversed
FROM frontier
WHERE
    timestamp_processed is not null AND
    hostname_reversed like reverse('%.de') or hostname_reversed=reverse('%.fr');

 *
 * The following version of this insertion is tailored to restart the search process of sinonk.com
 *
 
BEGIN;
    UPDATE frontier
    SET priority = priority-1000000
    WHERE
        hostname_reversed=reverse('.sinonk.com')
    ;
    INSERT INTO frontier (id_urls, priority, timestamp_received, hostname_reversed)
    SELECT DISTINCT ON (id_urls)
        articles.id_urls,
        0 as priority,
        current_timestamp,
        reverse(concat('.',hostname))
    FROM get_valid_articles2('sinonk.com') as va
    INNER JOIN articles ON va.id_articles = articles.id_articles
    ;
COMMIT;

 *
 * The following query reinserts languages into the frontier,
 * which we must do due to the modifications of keywords.csv
 *

INSERT INTO frontier (id_urls,priority,timestamp_received,hostname_reversed)
SELECT DISTINCT ON (id_urls)
    CASE WHEN articles.id_urls_canonical = 2425 
         THEN articles.id_urls 
         ELSE articles.id_urls_canonical 
    END AS id_urls,
    frontier.priority,
    now(),
    frontier.hostname_reversed
FROM articles
INNER JOIN responses ON responses.id_responses=articles.id_responses
INNER JOIN frontier ON frontier.id_frontier=responses.id_frontier
WHERE
    lang='ru' or 
    lang='zh' or 
    lang='ja' or 
    lang='kr' or 
    lang='tl' or 
    lang='vi' or 
    lang='pt' or
    lang='it' or
    lang='ar' or
    lang='la' or
    lang='id' or
    lang='sw' or
    lang='ku' or
    lang='gl' or
    lang='fa' or
    lang='he' or
    lang='yi' or
    lang='tr' or
    ((lang='de' or lang='fr') and pub_time <= '2020-01-01') 
;
*/

/* 
 * this code updates the null hostname 
 *
with new_values as (
    select responses.id_responses,urls.hostname from responses
    inner join frontier on frontier.id_frontier=responses.id_frontier
    inner join urls on urls.id_urls=frontier.id_urls
    where
        responses.hostname is null
    )
update responses
set hostname=new_values.hostname
from new_values
where responses.id_responses=new_values.id_responses;
*/

/*
 * this code increases the priority of important domains in the frontier
 *
update frontier
set priority=priority+10000000
where hostname_reversed=reverse('.www.nytimes.com');

update frontier
set priority=priority+50
where hostname_reversed=reverse('.www.usatoday.com');

update frontier
set priority=priority+100000
where hostname_reversed=reverse('.www.washingtonpost.com');

update frontier
set priority=-100
where hostname_reversed=reverse('.onfaith.washingtonpost.com');
*/

/*
 * Adjust how the frontier priorities are calculated so that 
 * duplicate urls are less likely
 *
update frontier
    set priority=priority-1000000
where
    id_frontier in (
        select id_frontier
        from frontier
        inner join urls on urls.id_urls = frontier.id_urls
        where
            query!='' or
            fragment!='' or
            params!=''
        );
*/

/*
select hostname,count(1) as c from (select hostname,id_urls_canonical,title,count(1) as c from articles group by hostname,id_urls_canonical,title) as t group by hostname order by c desc;

select hostname,id_urls,id_urls_canonical,title 
from articles
where id_urls_canonical!=2425
group by hostname,id_urls,id_urls_canonical,title
having (count(id_urls_canonical)>1)
order by id_urls_canonical
;
*/


/*
 * FIXME: we need to convert this into a function so that the inner where clause can be a parameter
 *
CREATE VIEW articles_deduped AS
    SELECT DISTINCT ON (text) *
    FROM (
        SELECT DISTINCT ON (title) * 
        FROM articles
        WHERE 
            hostname='www.northkoreatech.org' AND
            title != '' AND
            title IS NOT NULL AND
            text != '' AND
            text IS NOT NULL
        ) as dedupe_title
        ;
*/

select count(1) from (
SELECT DISTINCT ON (id_urls_canonical_) 
    urls.scheme,
    urls.hostname,
    urls.port,
    urls.path,
    urls.params,
    urls.query,
    urls.fragment,
    urls.other,
    articles.id_urls,
    articles.id_urls_canonical,
    CASE WHEN articles.id_urls_canonical = 2425 
         THEN articles.id_urls 
         ELSE articles.id_urls_canonical 
    END AS id_urls_canonical_
FROM articles
INNER JOIN urls ON urls.id_urls = articles.id_urls
WHERE
    --articles.pub_time is not null AND 
    articles.text is not null AND 
    articles.title is not null AND 
    articles.hostname = 'sinonk.com'
ORDER BY id_urls_canonical_
) t;

select distinct on (path)
    path
from urls
where hostname='sinonk.com';

SELECT count(1) 
FROM (
    SELECT DISTINCT articles.id_articles
    FROM sentences
    INNER JOIN articles on sentences.id_articles = articles.id_articles
    WHERE hostname='www.armscontrolwonk.com'
) AS t;

SELECT
    t1.timeunit,
    responses,
    articles,
    valid_articles,
    valid_articles/(1.0*articles) as va_per_a,
    valid_articles/(1.0*responses) as va_per_r,
    articles/(1.0*responses) as a_per_r
FROM (
    SELECT date_trunc('day',timestamp_processed) AS timeunit, count(1) AS valid_articles
    FROM get_valid_articles2('armscontrol.org') as t
    inner join articles on articles.id_articles = t.id_articles
    inner join responses on responses.id_responses = articles.id_responses
    group by timeunit
    ) as t1
INNER JOIN (
    SELECT date_trunc('day',timestamp_processed) AS timeunit, count(1) AS articles
    FROM (
        SELECT articles.id_articles
        FROM articles
        WHERE
            articles.pub_time IS NOT NULL AND
            articles.text IS NOT NULL AND
            articles.title IS NOT NULL AND
            articles.hostname = 'armscontrol.org'
    ) AS t
    INNER JOIN articles on articles.id_articles = t.id_articles
    INNER JOIN responses on responses.id_responses = articles.id_responses
    GROUP BY timeunit
    ) AS t2 ON t1.timeunit=t2.timeunit
INNER JOIN (
    select date_trunc('day',timestamp_processed) AS timeunit, count(1) AS responses 
    FROM responses 
    WHERE hostname = 'armscontrol.org'
    GROUP BY timeunit
    ) AS t3 ON t1.timeunit = t3.timeunit
ORDER BY timeunit DESC;

SELECT path,query
FROM articles
INNER JOIN responses on responses.id_responses = articles.id_responses
INNER JOIN urls on urls.id_urls = articles.id_urls
WHERE
    articles.hostname='sinonk.com' AND    
    timestamp_processed >= now() - interval '10 minutes'
ORDER BY timestamp_processed DESC
    ;

SELECT priority,path,query
FROM frontier 
INNER JOIN urls on urls.id_urls = frontier.id_urls
WHERE
    frontier.hostname_reversed=reverse('.sinonk.com') AND    
    timestamp_processed >= now() - interval '10 minutes'
ORDER BY timestamp_processed DESC
    ;

SELECT count(1)
FROM frontier
WHERE
    frontier.hostname_reversed=reverse('.sinonk.com') AND    
    priority >= 0
    ;


/*
 * this query finds entries in the frontier that have multiple responses
 */
select
    num,
    count(1) as total
from (
    select
        frontier.id_frontier,
        count(1) as num
    from frontier
    left join responses on responses.id_frontier=frontier.id_frontier
    where 
        frontier.id_frontier > 347000000 and
        frontier.id_frontier < 348000000
    group by frontier.id_frontier
    order by num desc
) t
group by num
order by num desc
;

select
    responses.id_responses,
    count(1) as num
from articles
left join responses on responses.id_responses=articles.id_responses
where 
group by responses.id_responses
order by num desc
;

/*
 *
 */
select substring(reverse(hostname_reversed) from 2) as hostname,count(1) 
from frontier 
where 
    timestamp_processed is null 
    and priority='inf' 
    and substring(reverse(hostname_reversed) from 2) not in (
        select hostname 
        from crawlable_hostnames
        where priority='ban'
    )
group by hostname 
order by count desc;

select substring(reverse(hostname_reversed) from 2) as hostname,priority
from frontier 
where 
    timestamp_processed is null 
    and priority<'inf' 
    and substring(reverse(hostname_reversed) from 2) not in (
        select hostname 
        from crawlable_hostnames
        where priority='ban'
    )
order by priority desc
limit 10000;


SELECT DISTINCT id_articles,title 
FROM articles 
WHERE 
    to_tsvector('english',title) @@ to_tsquery('north & korea & (rabbit | bunny)') 
    --and hostname='www.nytimes.com' 
    and lang='en'
LIMIT 20;


select 
    scheme || '://' || urls.hostname || path as url,
    pub_time
from urls 
inner join articles on articles.id_urls=urls.id_urls
where 
    --pub_time > now()
    pub_time < '1960-01-01'
    and articles.hostname='www.nytimes.com'
limit 10



SELECT 
    t1.hostname,
    responses,
    keywords_title,
    --(keywords_title/responses) AS fraction_title,
    COALESCE(ROUND(keywords_title/responses::numeric,4),0) AS title_per_response
    --keywords_text
    --(keywords_text/responses) AS fraction_text
    --COALESCE(ROUND(keywords_text/responses::numeric,4),0) AS text_per_response,
    --COALESCE(ROUND(keywords_title/keywords_text::numeric,4),0) AS title_per_text
FROM (
    SELECT hostname,count(1) AS keywords_title
    FROM (
        SELECT distinct on (hostname,title) id_articles,hostname,date_trunc('day',pub_time) as day,title
        FROM articles
        WHERE
            to_tsquery('english','(coronavirus | (corona & virus) | covid)') @@ to_tsvector('english',title)
            and lang='en'
            and pub_time is not null
        order by hostname,title,pub_time asc
        )t
    GROUP BY hostname 
    ) t1
INNER JOIN (
    SELECT hostname,sum(num) AS responses
    FROM responses_timestamp_hostname 
    WHERE timestamp > '2020-02-01' 
    GROUP BY hostname 
    )t3 ON t3.hostname=t1.hostname
ORDER BY title_per_response DESC,hostname DESC
;

SELECT 
    t1.hostname,
    responses,
    keywords_title,
    --(keywords_title/responses) AS fraction_title,
    COALESCE(ROUND(keywords_title/responses::numeric,4),0) AS title_per_response,
    keywords_text,
    --(keywords_text/responses) AS fraction_text
    COALESCE(ROUND(keywords_text/responses::numeric,4),0) AS text_per_response,
    COALESCE(ROUND(keywords_title/keywords_text::numeric,4),0) AS title_per_text
FROM (
    SELECT hostname,count(1) AS keywords_title
    FROM (
        SELECT distinct on (hostname,title) id_articles,hostname,date_trunc('day',pub_time) as day,title
        FROM articles
        WHERE
            to_tsquery('english','(coronavirus | (corona & virus) | covid)') @@ to_tsvector('english',title)
            and lang='en'
            and pub_time is not null
        order by hostname,title,pub_time asc
        )t
    GROUP BY hostname 
    ) t1
INNER JOIN (
    SELECT hostname,count(1) AS keywords_text
    FROM (
        SELECT distinct on (hostname,title) id_articles,hostname,date_trunc('day',pub_time) as day,title
        FROM articles
        WHERE
            to_tsquery('english','(coronavirus | (corona & virus) | covid)') @@ to_tsvector('english',text)
            and lang='en'
            and pub_time is not null
        order by hostname,title,pub_time asc
        )t
    GROUP BY hostname 
    ) t2 ON t2.hostname=t1.hostname
INNER JOIN (
    SELECT hostname,sum(num) AS responses
    FROM responses_timestamp_hostname 
    WHERE timestamp > '2020-02-01' 
    GROUP BY hostname 
    )t3 ON t3.hostname=t1.hostname
ORDER BY text_per_response DESC,title_per_response,hostname DESC
;


/*
 *
 */
CREATE VIEW vacuum_stats AS (
    WITH rel_set AS
    (
        SELECT
            oid,
            CASE split_part(split_part(array_to_string(reloptions, ','), 'autovacuum_vacuum_threshold=', 2), ',', 1)
                WHEN '' THEN NULL
            ELSE split_part(split_part(array_to_string(reloptions, ','), 'autovacuum_vacuum_threshold=', 2), ',', 1)::BIGINT
            END AS rel_av_vac_threshold,
            CASE split_part(split_part(array_to_string(reloptions, ','), 'autovacuum_vacuum_scale_factor=', 2), ',', 1)
                WHEN '' THEN NULL
            ELSE split_part(split_part(array_to_string(reloptions, ','), 'autovacuum_vacuum_scale_factor=', 2), ',', 1)::NUMERIC
            END AS rel_av_vac_scale_factor
        FROM pg_class
    )
    SELECT
        PSUT.relname,
        to_char(PSUT.last_vacuum, 'YYYY-MM-DD HH24:MI')     AS last_vacuum,
        to_char(PSUT.last_autovacuum, 'YYYY-MM-DD HH24:MI') AS last_autovacuum,
        to_char(C.reltuples, '999G999G999G999G999')               AS n_tup,
        to_char(PSUT.n_dead_tup, '999G999G999G999G999')           AS dead_tup,
        to_char(coalesce(RS.rel_av_vac_threshold, current_setting('autovacuum_vacuum_threshold')::BIGINT) + coalesce(RS.rel_av_vac_scale_factor, current_setting('autovacuum_vacuum_scale_factor')::NUMERIC) * C.reltuples, '999G999G999G999G999') AS av_threshold,
        to_char(age(C.relfrozenxid), '999G999G999G999G999') AS xid_age,
        to_char(current_setting('autovacuum_freeze_max_age')::Numeric, '999G999G999G999G999') AS autovacuum_freeze_max_age,
        age(C.relfrozenxid) / current_setting('autovacuum_freeze_max_age')::Numeric AS freeze_ratio,

        CASE
            WHEN (coalesce(RS.rel_av_vac_threshold, current_setting('autovacuum_vacuum_threshold')::BIGINT) + coalesce(RS.rel_av_vac_scale_factor, current_setting('autovacuum_vacuum_scale_factor')::NUMERIC) * C.reltuples) < PSUT.n_dead_tup
            THEN '*'
        ELSE ''
        END AS expect_av
    FROM
        pg_stat_user_tables PSUT
        JOIN pg_class C
            ON PSUT.relid = C.oid
        JOIN rel_set RS
            ON PSUT.relid = RS.oid
    ORDER BY C.reltuples DESC
);
