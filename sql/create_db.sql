PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS seed_hostnames (
    hostname VARCHAR(253) PRIMARY KEY,
    lang VARCHAR(2),
    country VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS urls (
    id_urls INTEGER PRIMARY KEY,
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
    id_frontier INTEGER PRIMARY KEY,
    id_urls INTEGER,
    priority REAL,
    timestamp_received DATETIME,
    timestamp_processed DATETIME,

    /* the contents of the hostname_reversed column must exactly be equal to 
     * the associated hostname entry in urls, but reversed and with a '.'
     * added to the last position.
     */
    hostname_reversed VARCHAR(253),

    FOREIGN KEY (id_urls) REFERENCES urls(id_urls)
);

CREATE INDEX IF NOT EXISTS frontier_index_urls ON frontier(id_urls);
CREATE INDEX IF NOT EXISTS frontier_index_nextrequest ON frontier(timestamp_processed,hostname_reversed,priority);

CREATE VIEW IF NOT EXISTS frontier_urls AS
    SELECT * 
    FROM frontier
    INNER JOIN urls ON frontier.id_urls=urls.id_urls;

CREATE TABLE IF NOT EXISTS responses (
    id_responses INTEGER PRIMARY KEY,
    id_frontier INTEGER, 
    id_urls_redirected INTEGER,
    timestamp_received DATETIME,
    timestamp_processed DATETIME,
    twisted_status VARCHAR(16),
    twisted_status_long VARCHAR(256),
    http_status VARCHAR(4),
    dataloss BOOLEAN,
    bytes INTEGER,
    FOREIGN KEY (id_frontier) REFERENCES frontier(id_frontier),
    FOREIGN KEY (id_urls_redirected) REFERENCES urls(id_urls)
);

CREATE INDEX IF NOT EXISTS responses_index_frontier ON responses(id_frontier);

CREATE VIEW IF NOT EXISTS responses_urls AS
    SELECT responses.*,urls.*
    FROM responses
    INNER JOIN frontier ON responses.id_frontier=frontier.id_frontier
    INNER JOIN urls ON frontier.id_urls=urls.id_urls;

/*
 * These tables store the actual scraped articles.
 */

CREATE TABLE IF NOT EXISTS articles (
    id_articles INTEGER PRIMARY KEY,
    id_urls INTEGER, 
    id_urls_canonical INTEGER, 
    id_responses INTEGER,
    title TEXT,
    alltext TEXT,
    text TEXT,
    lang VARCHAR(2),
    pub_time DATETIME,
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls),
    FOREIGN KEY (id_urls_canonical) REFERENCES urls(id_urls),
    FOREIGN KEY (id_responses) REFERENCES responses(id_responses)
);

CREATE VIEW IF NOT EXISTS articles_urls AS
    SELECT *
    FROM articles
    INNER JOIN urls ON articles.id_urls=urls.id_urls;

CREATE TABLE IF NOT EXISTS keywords (
    id_keywords INTEGER PRIMARY KEY,
    id_articles INTEGER, 
    keyword VARCHAR(16),
    num_title INTEGER,
    num_text INTEGER,
    num_alltext INTEGER,
    FOREIGN KEY (id_articles) REFERENCES articles(id_articles)
);

CREATE TABLE IF NOT EXISTS authors (
    id_authors INTEGER PRIMARY KEY,
    id_articles INTEGER, 
    author VARCHAR(128), 
    FOREIGN KEY (id_articles) REFERENCES articles(id_authors)
);

CREATE TABLE IF NOT EXISTS refs (
    id_refs INTEGER PRIMARY KEY,
    source INTEGER,
    target INTEGER,
    type VARCHAR(10),
    text VARCHAR(2084),

    FOREIGN KEY (source) REFERENCES articles(id_articles),
    FOREIGN KEY (target) REFERENCES urls(id_urls)
);

CREATE VIEW IF NOT EXISTS refs_urls AS
    SELECT 
          id_refs
        , source
        , target
        , type
        , text
        , source_urls.scheme as source_scheme
        , source_urls.hostname as source_hostname
        , source_urls.port as source_port
        , source_urls.path as source_hostname
        , source_urls.params as source_params
        , source_urls.query as source_query
        , source_urls.fragment as source_fragment
        , target_urls.scheme as target_scheme
        , target_urls.hostname as target_hostname
        , target_urls.port as target_port
        , target_urls.path as target_hostname
        , target_urls.params as target_params
        , target_urls.query as target_query
        , target_urls.fragment as target_fragment
        FROM refs
        INNER JOIN urls AS source_urls ON source_urls.id_urls=refs.source
        INNER JOIN urls AS target_urls ON target_urls.id_urls=refs.target
        ;
