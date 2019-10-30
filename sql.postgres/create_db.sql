CREATE TABLE IF NOT EXISTS seed_hostnames (
    hostname VARCHAR(253) PRIMARY KEY,
    lang VARCHAR(2),
    country VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS urls (
    id_urls BIGSERIAL PRIMARY KEY,
    scheme VARCHAR(8),
    hostname VARCHAR(253),
    port INTEGER,
    path VARCHAR(1024),
    params VARCHAR(256),
    query VARCHAR(1024),
    fragment VARCHAR(256),
    other VARCHAR(2048),
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
    id_urls INTEGER,
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

/*
CREATE VIEW IF NOT EXISTS frontier_urls AS
    SELECT * 
    FROM frontier
    INNER JOIN urls ON frontier.id_urls=urls.id_urls;
*/

CREATE TABLE IF NOT EXISTS responses (
    id_responses BIGSERIAL PRIMARY KEY,
    id_frontier INTEGER, 
    id_urls_redirected INTEGER,
    timestamp_received TIMESTAMP,
    timestamp_processed TIMESTAMP,
    twisted_status VARCHAR(256),
    twisted_status_long VARCHAR(2048),
    http_status VARCHAR(4),
    dataloss BOOLEAN,
    bytes INTEGER,
    FOREIGN KEY (id_frontier) REFERENCES frontier(id_frontier) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_urls_redirected) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS responses_index_frontier ON responses(id_frontier);
CREATE INDEX IF NOT EXISTS responses_index_timestamp_received ON responses(timestamp_received);
CREATE INDEX IF NOT EXISTS responses_index_timestamp_processed ON responses(timestamp_processed);

CREATE VIEW responses_status AS
    SELECT hostname,twisted_status,http_status,count(1) as num
    FROM responses
    INNER JOIN frontier ON frontier.id_frontier=responses.id_frontier
    INNER JOIN urls ON urls.id_urls=frontier.id_urls
    GROUP BY hostname,twisted_status,http_status
    ORDER BY hostname,twisted_status,http_status
    ;

CREATE VIEW crawl_performance AS
    SELECT t1.timestamp,frontier_received,frontier_processed,responses_received,responses_processed
    FROM (SELECT date_trunc('minute', timestamp_received) as timestamp,count(1) as frontier_received
        FROM frontier GROUP BY timestamp) AS t1
    INNER JOIN (SELECT date_trunc('minute', timestamp_processed) as timestamp,count(1) as frontier_processed
        FROM frontier GROUP BY timestamp) AS t2 ON t1.timestamp=t2.timestamp
    INNER JOIN (SELECT date_trunc('minute', timestamp_received) as timestamp,count(1) as responses_received 
        FROM responses GROUP BY timestamp) AS t3 ON t1.timestamp=t3.timestamp
    INNER JOIN (SELECT date_trunc('minute', timestamp_processed) as timestamp,count(1) as responses_processed
        FROM responses GROUP BY timestamp) AS t4 ON t1.timestamp=t4.timestamp
    ORDER BY t1.timestamp;

/*
CREATE VIEW IF NOT EXISTS responses_urls AS
    SELECT responses.*,urls.*
    FROM responses
    INNER JOIN frontier ON responses.id_frontier=frontier.id_frontier
    INNER JOIN urls ON frontier.id_urls=urls.id_urls;
*/

/*
 * These tables store the actual scraped articles.
 */

CREATE TABLE IF NOT EXISTS articles (
    id_articles BIGSERIAL PRIMARY KEY,
    id_urls INTEGER, 
    id_urls_canonical INTEGER, 
    id_responses INTEGER,
    title TEXT,
    alltext TEXT,
    text TEXT,
    lang VARCHAR(2),
    pub_time TIMESTAMP,
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_urls_canonical) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_responses) REFERENCES responses(id_responses) DEFERRABLE INITIALLY DEFERRED
);

/*
CREATE VIEW IF NOT EXISTS articles_urls AS
    SELECT *
    FROM articles
    INNER JOIN urls ON articles.id_urls=urls.id_urls;
*/

CREATE TABLE IF NOT EXISTS keywords (
    id_keywords BIGSERIAL PRIMARY KEY,
    id_articles INTEGER, 
    keyword VARCHAR(16),
    num_title INTEGER,
    num_text INTEGER,
    num_alltext INTEGER,
    FOREIGN KEY (id_articles) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS authors (
    id_authors BIGSERIAL PRIMARY KEY,
    id_articles INTEGER, 
    author VARCHAR(4096), 
    FOREIGN KEY (id_articles) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS refs (
    id_refs BIGSERIAL PRIMARY KEY,
    source INTEGER,
    target INTEGER,
    type VARCHAR(10),
    text VARCHAR(2084),

    FOREIGN KEY (source) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (target) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED
);

CREATE VIEW refs_urls AS
    SELECT 
          id_refs
        , source
        , target
        , type
        , text
        , source_urls.scheme as source_scheme
        , source_urls.hostname as source_hostname
        , source_urls.port as source_port
        , source_urls.path as source_path
        , source_urls.params as source_params
        , source_urls.query as source_query
        , source_urls.fragment as source_fragment
        , target_urls.scheme as target_scheme
        , target_urls.hostname as target_hostname
        , target_urls.port as target_port
        , target_urls.path as targete_path
        , target_urls.params as target_params
        , target_urls.query as target_query
        , target_urls.fragment as target_fragment
        FROM refs
        INNER JOIN urls AS source_urls ON source_urls.id_urls=refs.source
        INNER JOIN urls AS target_urls ON target_urls.id_urls=refs.target
        ;

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

CREATE VIEW crawl_responses_hostname AS 
    SELECT hostname,twisted_status,http_status,sum(num)
    FROM crawl_responses
    GROUP BY hostname,twisted_status,http_status;

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

CREATE VIEW articles_summary AS 
    SELECT 
        hostname,
        extract(YEAR FROM pub_time) as year,
        extract(MONTH FROM pub_time) as month,
        count(1) as num
    FROM articles
    INNER JOIN urls on urls.id_urls=articles.id_urls_canonical
    GROUP BY hostname,year,month
    ORDER BY hostname,year,month;
