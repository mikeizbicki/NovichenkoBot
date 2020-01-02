CREATE EXTENSION btree_gist;
CREATE EXTENSION tablefunc;

CREATE TABLE crawlable_hostnames (
    hostname VARCHAR(253) PRIMARY KEY,
    lang VARCHAR(2),
    country VARCHAR(2),
    priority TEXT
);

CREATE TABLE hostnames (
    id_hostnames SERIAL PRIMARY KEY,
    hostname VARCHAR(253) UNIQUE NOT NULL,
    lang VARCHAR(2),
    country VARCHAR(2),
    type VARCHAR(256),
    priority TEXT
);

CREATE TABLE urls (
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

CREATE INDEX urls_index_hostname_path ON urls(hostname,path);

/*
 * These tables store metadata about which urls have been 
 * scheduled and downloaded.
 */
CREATE TABLE frontier (
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

CREATE INDEX frontier_index_urls ON frontier(id_urls);
CREATE INDEX frontier_index_hostname_reversed ON frontier(hostname_reversed);
CREATE INDEX frontier_index_timestamp_received ON frontier(timestamp_received);
CREATE INDEX frontier_index_nextrequest ON frontier(timestamp_processed,hostname_reversed,priority,id_frontier,id_urls);
CREATE INDEX frontier_index_nextrequest_alt ON frontier(priority) WHERE timestamp_processed IS NULL;

/* FIXME:
CREATE TABLE requests (
    id_requests BIGSERIAL PRIMARY KEY,
    id_frontier BIGINT,
    timestamp TIMESTAMP
);
*/

CREATE TABLE responses (
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

-- FIXME: 
-- somehow there are duplicate rows in responses for the same id_frontier;
-- we need to find how this happens in the spider code and fix that;
-- figure out which of the duplicate rows to get rid of and which to keep;
-- and then fix this unique constraint to apply to all rows.
-- Now that this constraint is added, we should start getting failures in the 
-- scrapy logs which will tell us what is causing the duplication.
create unique index responses_unique_frontier on responses(id_frontier)
where id_responses > 111562143;

CREATE INDEX responses_index_frontier ON responses(id_frontier);
CREATE INDEX responses_index_timestamp_received ON responses(timestamp_received);
CREATE INDEX responses_index_timestamp_processed ON responses(timestamp_processed);
CREATE INDEX responses_index_hostnametwistedhttp ON responses(hostname,twisted_status,http_status);

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

CREATE TABLE articles (
    id_articles BIGSERIAL PRIMARY KEY,
    id_responses BIGINT NOT NULL, --UNIQUE
    id_urls BIGINT NOT NULL, 
    id_urls_canonical BIGINT NOT NULL, 
    hostname VARCHAR(253) NOT NULL,
    title TEXT,
    text TEXT,
    html TEXT,
    lang VARCHAR(2),
    pub_time TIMESTAMP,
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_urls_canonical) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_responses) REFERENCES responses(id_responses) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX articles_index_urls ON articles(id_urls);
CREATE INDEX articles_index_hostnametime ON articles(hostname,pub_time);
-- FIXME: 
-- CREATE INDEX concurrently articles_title_tsv ON articles USING GIST (to_tsvector('english',title));
--CREATE INDEX concurrently articles_text_tsv ON articles USING GIST (to_tsvector('english',text));

CREATE FUNCTION get_valid_articles(_hostname TEXT)
RETURNS TABLE 
    ( id_articles BIGINT
    , id_urls_canonical_ BIGINT
    , pub_time TIMESTAMP
    , lang VARCHAR(2)
    , text TEXT
    , title TEXT
    ) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (id_urls_canonical_) 
        articles.id_articles,
        CASE WHEN articles.id_urls_canonical = 2425 THEN articles.id_urls ELSE articles.id_urls_canonical END as id_urls_canonical_,
        articles.pub_time,
        articles.lang,
        articles.text,
        articles.title
    FROM articles
    WHERE
        articles.pub_time is not null AND
        articles.text is not null AND
        articles.title is not null AND
        articles.hostname = _hostname
    ORDER BY id_urls_canonical_; 
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_valid_articles2(_hostname TEXT)
RETURNS TABLE 
    ( id_articles BIGINT
    , id_urls_canonical_ BIGINT
    , pub_time TIMESTAMP
    , lang VARCHAR(2)
    , text TEXT
    , title TEXT
    ) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (text) * FROM (
        SELECT DISTINCT ON (title) * FROM (
            SELECT DISTINCT ON (id_urls_canonical_) 
                articles.id_articles,
                CASE WHEN articles.id_urls_canonical = 2425 THEN articles.id_urls ELSE articles.id_urls_canonical END as id_urls_canonical_,
                articles.pub_time,
                articles.lang,
                articles.text,
                articles.title
            FROM articles
            WHERE
                articles.pub_time is not null AND
                articles.text is not null AND
                articles.title is not null AND
                articles.hostname = _hostname
            ) AS t1
        ) AS t2
    ORDER BY text,id_urls_canonical_; 
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_articles_performance(_hostname TEXT)
RETURNS TABLE 
    ( timeunit TIMESTAMP
    , responses BIGINT
    , articles BIGINT
    , valid_articles BIGINT
    , valid_articles2 BIGINT
    , va2_per_r NUMERIC
    , va2_per_a NUMERIC
    , va2_per_va NUMERIC
    , a_per_r NUMERIC
    ) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t_va.timeunit,
        t_r.responses,
        t_a.articles,
        t_va.valid_articles,
        t_va2.valid_articles2,
        TRUNC(t_va2.valid_articles2/(1.0*t_r.responses),4) as va2_per_r,
        TRUNC(t_va2.valid_articles2/(1.0*t_a.articles),4) as va2_per_a,
        TRUNC(t_va2.valid_articles2/(1.0*t_va.valid_articles),4) as va2_per_va,
        TRUNC(t_a.articles/(1.0*t_r.responses),4) as a_per_r
    FROM (
        SELECT date_trunc('day',timestamp_processed) AS timeunit, count(1) AS valid_articles
        FROM get_valid_articles(_hostname) as t
        inner join articles on articles.id_articles = t.id_articles
        inner join responses on responses.id_responses = articles.id_responses
        group by timeunit
        ) as t_va
    INNER JOIN (
        SELECT date_trunc('day',timestamp_processed) AS timeunit, count(1) AS valid_articles2
        FROM get_valid_articles2(_hostname) as t
        inner join articles on articles.id_articles = t.id_articles
        inner join responses on responses.id_responses = articles.id_responses
        group by timeunit
        ) AS t_va2 ON t_va.timeunit=t_va2.timeunit
    INNER JOIN (
        SELECT date_trunc('day',timestamp_processed) AS timeunit, count(1) AS articles
        FROM (
            SELECT articles.id_articles
            FROM articles
            WHERE
                articles.hostname = _hostname
        ) AS t
        INNER JOIN articles on articles.id_articles = t.id_articles
        INNER JOIN responses on responses.id_responses = articles.id_responses
        GROUP BY timeunit
        ) AS t_a ON t_va.timeunit=t_a.timeunit
    INNER JOIN (
        select date_trunc('day',timestamp_processed) AS timeunit, count(1) AS responses 
        FROM responses 
        WHERE hostname = _hostname
        GROUP BY timeunit
        ) AS t_r ON t_a.timeunit = t_r.timeunit
    ORDER BY timeunit DESC;
END
$$ LANGUAGE plpgsql;

CREATE TABLE articles_valid (
    id_articles BIGINT,
    PRIMARY KEY(id_articles),
    FOREIGN KEY(id_articles) REFERENCES articles(id_articles)
);

CREATE VIEW articles_per_year AS 
    SELECT 
        hostname,
        extract(YEAR FROM pub_time) as year,
        count(1) as num
    FROM articles
    GROUP BY hostname,year
    ORDER BY hostname,year;

CREATE TABLE keywords (
    id_keywords BIGSERIAL PRIMARY KEY,
    id_articles BIGINT, 
    keyword VARCHAR(16),
    num_title INTEGER,
    num_text INTEGER,
    num_alltext INTEGER,
    FOREIGN KEY (id_articles) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE authors (
    id_authors BIGSERIAL PRIMARY KEY,
    id_articles BIGINT, 
    author VARCHAR(4096), 
    FOREIGN KEY (id_articles) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE refs (
    id_refs BIGSERIAL PRIMARY KEY,
    source BIGINT NOT NULL,
    target BIGINT, -- NOTE: this column is NULL whenever a target URL does not match the constraints of the urls table
    type VARCHAR(10),
    text VARCHAR(2084),
    FOREIGN KEY (source) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (target) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX refs_index_source ON refs(source);

/* FIXME:
CREATE TABLE refs2 (
    source BIGINT NOT NULL,
    target BIGINT,
    type VARCHAR(10),
    text VARCHAR(2048),
);

CREATE TABLE hostnames (
    
);
*/

/*
 * This function returns the article associated with a url;
 * this task is not as simple as looking up the id_urls associated with the url in the articles table;
 * because of redirects, there may be many urls that point to a single article,
 * but the id_urls field of articles only points to a single one of these urls;
 * given a url, this function will follow all redirects of that url to eventually find the article.
 */
CREATE FUNCTION id_urls_2_id_articles(bigint) RETURNS bigint
    AS 
    '
    SELECT id_articles 
    FROM articles
    WHERE id_urls IN (
        WITH RECURSIVE follow_redirects AS (
            SELECT frontier.id_urls,id_urls_redirected
            FROM responses
            INNER JOIN frontier ON frontier.id_frontier=responses.id_frontier
            WHERE id_urls=$1
            UNION
            SELECT follow_redirects.id_urls_redirected, responses.id_urls_redirected
            FROM responses
            INNER JOIN frontier ON frontier.id_frontier=responses.id_frontier
            INNER JOIN follow_redirects ON follow_redirects.id_urls_redirected=frontier.id_urls
        ) SELECT id_urls FROM follow_redirects WHERE id_urls_redirected IS NULL)
    '
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

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
    id_articles BIGINT,
    id_sentences INTEGER,
    sentence TEXT,
    sentence_resolved TEXT,
    PRIMARY KEY (id_articles,id_sentences),
    FOREIGN KEY (id_articles) REFERENCES articles(id_articles) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX sentences_ids_tsv ON sentences USING GIST (id_articles,id_sentence,to_tsvector('english', sentence_resolved));

CREATE TABLE labels (
    id_articles BIGINT,
    id_sentences INTEGER,
    id_labels VARCHAR(1024),
    score REAL,
    PRIMARY KEY (id_articles,id_sentences,id_labels),
    FOREIGN KEY (id_articles,id_sentences) REFERENCES sentences(id_articles,id_sentences) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_articles) REFERENCES articles(id_articles) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED 
);

