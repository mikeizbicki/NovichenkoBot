CREATE TABLE IF NOT EXISTS seed_hostnames (
    hostname VARCHAR(253) PRIMARY KEY,
    lang VARCHAR(2),
    country VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS urls (
    id_urls BIGSERIAL PRIMARY KEY,
    scheme VARCHAR(8) NOT NULL,
    hostname VARCHAR(253) NOT NULL,
    port INTEGER NOT NULL,
    path VARCHAR(1024) NOT NULL,
    params VARCHAR(256) NOT NULL,
    query VARCHAR(1024) NOT NULL,
    fragment VARCHAR(256) NOT NULL,
    other VARCHAR(2048) NOT NULL,
    depth INTEGER,
    UNIQUE(scheme,hostname,port,path,params,query,fragment,other)
);

CREATE INDEX IF NOT EXISTS urls_index_hostname ON urls(hostname);
CREATE INDEX IF NOT EXISTS urls_index_hostname_path ON urls(hostname,path);

/*
 * These tables store metadata about which urls have been 
 * scheduled and downloaded.
 */
CREATE TABLE IF NOT EXISTS frontier (
    id_frontier BIGSERIAL PRIMARY KEY,
    id_urls BIGINT,
    priority REAL,
    timestamp_received TIMESTAMP,
    timestamp_processed TIMESTAMP,

    /* the contents of the hostname_reversed column must exactly be equal to 
     * the associated hostname entry in urls, but reversed and with a '.'
     * added to the last position.
     */
    hostname_reversed VARCHAR(253),

    FOREIGN KEY (id_urls) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS frontier_index_urls ON frontier(id_urls);
CREATE INDEX IF NOT EXISTS frontier_index_timestamp_received ON frontier(timestamp_received);
CREATE INDEX IF NOT EXISTS frontier_index_timestamp_processed ON frontier(timestamp_processed);
CREATE INDEX IF NOT EXISTS frontier_index_nextrequest ON frontier(timestamp_processed,hostname_reversed,priority);
CREATE INDEX IF NOT EXISTS frontier_index_nextrequest2 ON frontier(timestamp_processed,hostname_reversed,priority,id_frontier,id_urls);

CREATE TABLE IF NOT EXISTS responses (
    id_responses BIGSERIAL PRIMARY KEY,
    id_frontier BIGINT NOT NULL, 
    hostname VARCHAR(253) NOT NULL,
    id_urls_redirected BIGINT,
    timestamp_received TIMESTAMP NOT NULL,
    timestamp_processed TIMESTAMP,
    twisted_status VARCHAR(256),
    twisted_status_long VARCHAR(2048),
    http_status VARCHAR(4),
    dataloss BOOLEAN,
    bytes INTEGER,
    recycled_into_frontier BOOLEAN DEFAULT false,
    FOREIGN KEY (id_frontier) REFERENCES frontier(id_frontier) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_urls_redirected) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS responses_index_frontier ON responses(id_frontier);
CREATE INDEX IF NOT EXISTS responses_index_timestamp_received ON responses(timestamp_received);
CREATE INDEX IF NOT EXISTS responses_index_timestamp_processed ON responses(timestamp_processed);
CREATE INDEX IF NOT EXISTS responses_index_hostnametwistedhttp ON responses(hostname,twisted_status,http_status);

CREATE VIEW total_byes AS
    SELECT pg_size_pretty(sum(bytes)) FROM responses;

/* this view lets us evaluate the performance of the crawler on particular domains */
CREATE VIEW responses_hostname AS
    SELECT hostname,twisted_status,http_status,count(1) as num
    FROM responses
    WHERE NOT recycled_into_frontier
    GROUP BY hostname,twisted_status,http_status
    ORDER BY hostname,twisted_status,http_status
    ;

CREATE VIEW responses_recent_status AS
    SELECT twisted_status,http_status,count(1) as num
    FROM responses
    WHERE
        timestamp_received>(SELECT max(timestamp_received)-interval '1 hour' FROM responses)
    GROUP BY twisted_status,http_status
    ORDER BY twisted_status,http_status
    ;

/* this view lets us evaluate the recent performance of the crawler */
CREATE VIEW responses_recent_performance AS
    SELECT
        t1.timestamp,
        responses_received,
        responses_processed,
        bytes
    FROM (
        SELECT 
            date_trunc('minute',timestamp_received) as timestamp,
            count(1) as responses_received 
        FROM responses
        WHERE timestamp_received>(SELECT max(timestamp_received)-interval '1 hour' FROM responses)
        GROUP BY timestamp
    ) AS t1
    FULL OUTER JOIN (
        SELECT 
            date_trunc('minute',timestamp_processed) as timestamp,
            count(1) as responses_processed ,
            sum(bytes) as bytes
        FROM responses
        WHERE timestamp_processed>(SELECT max(timestamp_received)-interval '1 hour' FROM responses)
        GROUP BY timestamp
    ) AS t2 ON t1.timestamp=t2.timestamp 
    ORDER BY t1.timestamp DESC;

CREATE VIEW responses_recent_performance2 AS
    SELECT
        t1.hostname,
        t1.timestamp,
        responses_received,
        responses_processed,
        bytes
    FROM (
        SELECT 
            hostname,
            date_trunc('minute',timestamp_received) as timestamp,
            count(1) as responses_received 
        FROM responses
        WHERE timestamp_received>(SELECT max(timestamp_received)-interval '1 hour' FROM responses)
        GROUP BY hostname,timestamp
    ) AS t1
    FULL OUTER JOIN (
        SELECT 
            hostname,
            date_trunc('minute',timestamp_processed) as timestamp,
            count(1) as responses_processed ,
            sum(bytes) as bytes
        FROM responses
        WHERE timestamp_processed>(SELECT max(timestamp_received)-interval '1 hour' FROM responses)
        GROUP BY hostname,timestamp
    ) AS t2 ON t1.timestamp=t2.timestamp and t1.hostname=t2.hostname
    ORDER BY t1.timestamp DESC;

/* If a response indicates that the URL failed to download for a "transient"
 * reason, we can recycle the response back into the frontier to try downloading again.
 */
CREATE FUNCTION responses_recycle()
RETURNS void AS $$
BEGIN
    CREATE TEMP TABLE responses_to_update AS
    SELECT id_urls,priority,hostname_reversed,id_responses
        FROM responses_recyclable
        INNER JOIN frontier ON frontier.id_frontier=responses_recyclable.id_frontier;

    INSERT INTO frontier
        (id_urls,priority,hostname_reversed,timestamp_received)
    SELECT id_urls,priority+1,hostname_reversed,now() from responses_to_update;

    UPDATE responses 
    SET recycled_into_frontier=true 
    WHERE id_responses IN (SELECT id_responses FROM responses_to_update);

    DROP TABLE responses_to_update;
END;
$$ LANGUAGE plpgsql;

CREATE VIEW responses_recyclable AS
    SELECT *
    FROM responses
    WHERE
        not recycled_into_frontier and
        twisted_status != 'Success' and
        twisted_status != 'IgnoreRequest' and
        twisted_status != 'DNSLookupError' and
        timestamp_processed is null and
        timestamp_received < now()-interval '10 minutes';

/*
 * A URL in the frontier is a ghost if its timestamp_processed is not null
 * but it has no entry in the responses table.  This can happen when
 * an exception is uncaught in the response code or when the crawler is
 * terminated unexpectedly.
 *
 * This function sets the timestamp_processed to null for each of these 
 * ghosted frontier urls so that they can be recrawled correctly.
 */
CREATE FUNCTION frontier_deghost()
RETURNS void AS $$
BEGIN
    UPDATE frontier 
    SET timestamp_processed=null
    WHERE id_frontier IN (SELECT id_frontier FROM frontier_ghost);
END;
$$ LANGUAGE plpgsql;

CREATE VIEW frontier_ghost AS
    SELECT frontier.*
    FROM frontier 
    LEFT JOIN responses ON responses.id_frontier=frontier.id_frontier
    WHERE
        responses.id_frontier is null and
        frontier.timestamp_processed is not null and
        frontier.timestamp_received < now()-interval '10 minutes';

/*
 * These tables store the actual scraped articles.
 */

CREATE TABLE IF NOT EXISTS articles (
    id_articles BIGSERIAL PRIMARY KEY,
    id_responses BIGINT NOT NULL,
    id_urls BIGINT NOT NULL, 
    id_urls_canonical BIGINT NOT NULL, 
    hostname VARCHAR(253) NOT NULL,
    title TEXT,
    alltext TEXT,
    text TEXT,
    html TEXT,
    lang VARCHAR(2),
    pub_time TIMESTAMP,
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_urls_canonical) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_responses) REFERENCES responses(id_responses) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX articles_index_hostnametime ON articles(hostname,pub_time);

CREATE VIEW articles_per_year AS 
    SELECT 
        hostname,
        extract(YEAR FROM pub_time) as year,
        count(1) as num
    FROM articles
    GROUP BY hostname,year
    ORDER BY hostname,year;

CREATE TABLE IF NOT EXISTS keywords (
    id_keywords BIGSERIAL PRIMARY KEY,
    id_articles BIGINT, 
    keyword VARCHAR(16),
    num_title INTEGER,
    num_text INTEGER,
    num_alltext INTEGER,
    FOREIGN KEY (id_articles) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS authors (
    id_authors BIGSERIAL PRIMARY KEY,
    id_articles BIGINT, 
    author VARCHAR(4096), 
    FOREIGN KEY (id_articles) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS refs (
    id_refs BIGSERIAL PRIMARY KEY,
    source BIGINT, -- FIXME: add not null constraint
    target BIGINT, -- NOTE: this column is NULL whenever a target URL does not match the constraints of the urls table
    type VARCHAR(10),
    text VARCHAR(2084),
    FOREIGN KEY (source) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (target) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED
);

/* This view gives us insight into how far along we are into the crawl for each domain.
 * FIXME: This would probably be better done as a roll-up table that is progressively
 * updated and can show how the crawl has progressed over time.
 */
CREATE MATERIALIZED VIEW fraction_crawled AS 
    SELECT
        t1.hostname,
        frontier_unique,
        articles_unique,
        articles_unique/(1.0*frontier_unique) as fraction_crawled
    FROM (
        SELECT
            hostname,
            count(1) as frontier_unique
        FROM (
            SELECT scheme,urls.hostname,urls.path,count(1) FROM frontier
            INNER JOIN urls ON urls.id_urls=frontier.id_urls
            GROUP BY urls.scheme,urls.hostname,urls.path
            ) AS t1a
            GROUP BY hostname
        ) AS t1
    LEFT OUTER JOIN (
        SELECT
            hostname,
            count(1) as articles_unique
        FROM (
            SELECT scheme,urls.hostname,urls.path,count(1) FROM articles
            INNER JOIN urls ON urls.id_urls=articles.id_urls
            GROUP BY urls.scheme,urls.hostname,urls.path
            ) AS t2a
            GROUP BY hostname
        ) AS t2 ON t1.hostname=t2.hostname
    WHERE articles_unique>1000
    ORDER BY fraction_crawled DESC
WITH NO DATA;
REFRESH MATERIALIZED VIEW fraction_crawled;

/*
 * The following tables are for postprocessing after download.
 */

CREATE TABLE sentences (
    id_sentences BIGSERIAL PRIMARY KEY,
    id_articles INTEGER,
    sentence TEXT,
    FOREIGN KEY (id_articles) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE labels (
    id_labels BIGSERIAL PRIMARY KEY,
    id_sentences INTEGER,
    type TEXT,
    source TEXT,
    score REAL,
    FOREIGN KEY (id_sentences) REFERENCES sentences(id_sentences) DEFERRABLE INITIALLY DEFERRED
);

/*********************************************************************************
 *
 * FIXME: Should we drop these tables now that hostname is incorporated
 * as a column in responses/articles?
 */

/*
 * The following "rollup tables" provide incremental summary stats.
 */

CREATE TABLE crawl_performance (
    hostname VARCHAR(253),
    timestamp TIMESTAMP,
    frontier_received INT,
    frontier_processed INT,
    --frontier_disappear INT,
    responses_received INT,
    responses_processed INT,
    responses_failures INT,
    responses_bytes INT,
    num_articles INT,
    PRIMARY KEY (hostname,timestamp)
);

CREATE INDEX crawl_performance_index_timestamp ON crawl_performance(timestamp);
CREATE INDEX crawl_performance_index_hostname ON crawl_performance(hostname);

CREATE VIEW crawl_performance_hostname AS
    SELECT hostname,
        sum(frontier_received) as frontier_received,
        sum(frontier_processed) as frontier_processed,
        --sum(frontier_disappear) as frontier_disappear,
        sum(responses_received) as responses_received,
        sum(responses_processed) as responses_processed,
        sum(responses_failures) as responses_failures,
        sum(responses_bytes) as responses_bytes,
        sum(num_articles) as num_articles
    FROM crawl_performance
    GROUP BY hostname 
    ORDER BY hostname ASC;

CREATE VIEW crawl_performance_timestamp AS
    SELECT timestamp,
        sum(frontier_received) as frontier_received,
        sum(frontier_processed) as frontier_processed,
        --sum(frontier_disappear) as frontier_disappear,
        sum(responses_received) as responses_received,
        sum(responses_processed) as responses_processed,
        sum(responses_failures) as responses_failures,
        sum(responses_bytes) as responses_bytes,
        sum(num_articles) as num_articles
    FROM crawl_performance
    GROUP BY timestamp
    ORDER BY timestamp DESC;

CREATE VIEW crawl_performance_hour AS
    SELECT date_trunc('hour',timestamp) as hour,
        sum(frontier_received) as frontier_received,
        sum(frontier_processed) as frontier_processed,
        sum(responses_received) as responses_received,
        sum(responses_processed) as responses_processed,
        sum(responses_failures) as responses_failures,
        sum(responses_bytes) as responses_bytes,
        sum(num_articles) as num_articles
    FROM crawl_performance
    GROUP BY hour
    ORDER BY hour DESC;

CREATE FUNCTION crawl_performance_update()
RETURNS void AS $$
BEGIN
    -- the most recent additions to crawl_performance are likely to be slightly
    -- out of date and need recomputing; therefore we delete them before
    -- updating the table
    DELETE FROM crawl_performance WHERE timestamp > (SELECT max(timestamp)-interval '2 minutes' FROM crawl_performance);
    INSERT INTO crawl_performance 
        ( SELECT * 
            FROM crawl_performance_new
        )
        ON CONFLICT (hostname,timestamp) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

        --SELECT 
            --hostname,date_trunc('minute', frontier.timestamp_received) as timestamp,
            --count(1) as frontier_received
        --FROM (SELECT id_urls,timestamp_received FROM frontier  WHERE frontier.timestamp_received>(SELECT max(timestamp) FROM crawl_performance)) as frontier_filter
        --INNER JOIN urls on frontier_filter.id_urls=urls.id_urls
        --GROUP BY hostname,timestamp

    select count(1) from (
        SELECT
            hostname,date_trunc('minute', frontier.timestamp_received) as timestamp,
            count(1) as frontier_received
        FROM frontier
        INNER JOIN urls on frontier.id_urls=urls.id_urls
        WHERE frontier.timestamp_received > (SELECT max(timestamp) FROM crawl_performance)
        GROUP BY hostname,timestamp
    );

        SELECT
            hostname,date_trunc('minute',frontier.timestamp_received) as timestamp,
            count(1) as frontier_received
        FROM frontier
        WHERE (hostname,frontier.id_urls) in (select hostname,id_urls from urls);


CREATE VIEW crawl_performance_new AS
    SELECT 
        t1.hostname,
        t1.timestamp,
        frontier_received,
        frontier_processed,
        responses_received,
        responses_processed,
        responses_failures,
        responses_bytes,
        num_articles
    FROM (
        SELECT 
            hostname,date_trunc('minute', frontier.timestamp_received) as timestamp,
            count(1) as frontier_received
        FROM frontier 
        INNER JOIN urls on frontier.id_urls=urls.id_urls
        WHERE frontier.timestamp_received > (SELECT max(timestamp) FROM crawl_performance)
        GROUP BY hostname,timestamp
    ) AS t1
    FULL OUTER JOIN (
        SELECT 
            hostname,date_trunc('minute', frontier.timestamp_processed) as timestamp,
            count(1) as frontier_processed
        FROM frontier 
        INNER JOIN urls on frontier.id_urls=urls.id_urls
        WHERE frontier.timestamp_processed > (SELECT max(timestamp) FROM crawl_performance)
        GROUP BY hostname,timestamp
    ) AS t2 ON t1.timestamp=t2.timestamp and t1.hostname=t2.hostname
    FULL OUTER JOIN (
        SELECT 
            hostname,date_trunc('minute', responses.timestamp_received) as timestamp,
            count(1) as responses_received 
        FROM responses 
        INNER JOIN frontier on frontier.id_frontier=responses.id_frontier
        INNER JOIN urls on urls.id_urls=frontier.id_urls
        WHERE responses.timestamp_received > (SELECT max(timestamp) FROM crawl_performance)
        GROUP BY hostname,timestamp
    ) AS t4 ON t1.timestamp=t4.timestamp and t1.hostname=t4.hostname
    FULL OUTER JOIN (
        SELECT 
            hostname,date_trunc('minute', responses.timestamp_processed) as timestamp,
            count(1) as responses_processed,
            sum(bytes) as responses_bytes
        FROM responses 
        INNER JOIN frontier on frontier.id_frontier=responses.id_frontier
        INNER JOIN urls on urls.id_urls=frontier.id_urls
        WHERE responses.timestamp_processed > (SELECT max(timestamp) FROM crawl_performance)
        GROUP BY hostname,timestamp
    ) AS t5 ON t1.timestamp=t5.timestamp and t1.hostname=t5.hostname
    FULL OUTER JOIN (
        SELECT 
            hostname,date_trunc('minute', responses.timestamp_received) as timestamp,
            count(1) as responses_failures
        FROM responses 
        INNER JOIN frontier on frontier.id_frontier=responses.id_frontier
        INNER JOIN urls on urls.id_urls=frontier.id_urls
        WHERE twisted_status!='Success' and responses.timestamp_received > (SELECT max(timestamp) FROM crawl_performance)
        GROUP BY hostname,timestamp
    ) AS t6 ON t1.timestamp=t6.timestamp and t1.hostname=t6.hostname
    FULL OUTER JOIN (
        SELECT 
            hostname,date_trunc('minute', responses.timestamp_processed) as timestamp,
            count(1) as num_articles
        FROM responses 
        INNER JOIN frontier on frontier.id_frontier=responses.id_frontier
        INNER JOIN articles on articles.id_responses=responses.id_responses
        INNER JOIN urls on urls.id_urls=frontier.id_urls
        WHERE responses.timestamp_processed > (SELECT max(timestamp) FROM crawl_performance)
        GROUP BY hostname,timestamp
    ) AS t8 ON t1.timestamp=t8.timestamp and t1.hostname=t8.hostname
    WHERE t1.hostname is not null and t1.timestamp is not null
    ORDER BY t1.hostname,t1.timestamp;


/**************************************/

CREATE TABLE crawl_responses (
    timestamp TIMESTAMP,
    hostname VARCHAR(253),
    twisted_status VARCHAR(256),
    http_status VARCHAR(4),
    num INTEGER,
    PRIMARY KEY (hostname,timestamp,twisted_status,http_status)
);

CREATE VIEW crawl_responses_hostname2 AS 
    SELECT hostname,twisted_status,http_status,sum(num)
    FROM crawl_responses
    GROUP BY hostname,twisted_status,http_status;

CREATE VIEW crawl_responses_hostname AS 
    SELECT
        hostname,
        sum(Success) as Success,
        sum(TCPTimedOutError) as TCPTimedOutError,
        sum(ResponseNeverReceived) as ResponseNeverReceived,
        sum(IgnoreRequest) as IgnoreRequest,
        sum(num-Success-TCPTimedOutError-ResponseNeverReceived-IgnoreRequest) as Other
    FROM (
        SELECT 
            hostname,
            CASE WHEN twisted_status='Success' THEN num ELSE 0 END as Success,
            CASE WHEN twisted_status='TCPTimedOutError' THEN num ELSE 0 END as TCPTimedOutError,
            CASE WHEN twisted_status='ResponseNeverReceived' THEN num ELSE 0 END as ResponseNeverReceived,
            CASE WHEN twisted_status='IgnoreRequest' THEN num ELSE 0 END as IgnoreRequest,
            num
        FROM crawl_responses
    ) AS t1
    GROUP BY hostname
    ORDER BY hostname;

CREATE VIEW crawl_responses_timestamp AS 
    SELECT
        timestamp,
        sum(Success) as Success,
        sum(TCPTimedOutError) as TCPTimedOutError,
        sum(ResponseNeverReceived) as ResponseNeverReceived,
        sum(IgnoreRequest) as IgnoreRequest,
        sum(num-Success-TCPTimedOutError-ResponseNeverReceived-IgnoreRequest) as Other
    FROM (
        SELECT 
            timestamp,
            CASE WHEN twisted_status='Success' THEN num ELSE 0 END as Success,
            CASE WHEN twisted_status='TCPTimedOutError' THEN num ELSE 0 END as TCPTimedOutError,
            CASE WHEN twisted_status='ResponseNeverReceived' THEN num ELSE 0 END as ResponseNeverReceived,
            CASE WHEN twisted_status='IgnoreRequest' THEN num ELSE 0 END as IgnoreRequest,
            num
        FROM crawl_responses
    ) AS t1
    GROUP BY timestamp
    ORDER BY timestamp;

CREATE FUNCTION crawl_responses_update()
RETURNS void AS $$
BEGIN
    -- the most recent additions to crawl_performance are likely to be slightly
    -- out of date and need recomputing; therefore we delete them before
    -- updating the table
    DELETE FROM crawl_responses WHERE timestamp > (SELECT max(timestamp)-interval '1 hour' FROM crawl_responses);
    INSERT INTO crawl_responses (SELECT * FROM crawl_responses_new);
END;
$$ LANGUAGE plpgsql;

CREATE VIEW crawl_responses_new AS
    SELECT 
        date_trunc('hour',responses.timestamp_received) as hour,
        hostname,
        twisted_status,
        coalesce(http_status,''),
        count(1) 
    FROM responses
    INNER JOIN frontier on frontier.id_frontier=responses.id_frontier
    INNER JOIN urls on urls.id_urls=frontier.id_urls
    WHERE
        --twisted_status is not null
        responses.timestamp_received > (SELECT max(timestamp)+interval '1 hour' FROM crawl_responses) 
    GROUP BY hour,hostname,twisted_status,http_status;

/**************************************/


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
set priority=priority+50
where hostname_reversed='moc.semityn.www.';

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
 * calculate the diskspace used by a column in 
 * from: https://stackoverflow.com/questions/18316893/how-to-estimate-the-size-of-one-column-in-a-postgres-table

select
    pg_size_pretty(sum(pg_column_size(alltext))) as total_size,
    pg_size_pretty(avg(pg_column_size(alltext))) as average_size,
    sum(pg_column_size(alltext)) * 100.0 / pg_total_relation_size('articles') as percentage
from articles;
*/
 
