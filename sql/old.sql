/*
 * These rollup tables are retained only for reference
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


