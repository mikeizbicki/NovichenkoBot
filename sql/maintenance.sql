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
AND blockinga.datname = current_database()

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
