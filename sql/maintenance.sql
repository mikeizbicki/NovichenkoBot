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
       s.idx_scan
FROM pg_catalog.pg_stat_user_indexes s
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
    total_time/calls as time_per_call, 
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
    hostname_reversed=reverse('.thediplomat.com');

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
