/*
 * FIXME: These cache tables and trigger rules assume that the tables
 * are only modified through the code in `scheduler.py`.
 * There is probably a way to modify these rules to make them more
 * robust to other changes as well.
 */

/*
 * This cache table stores the total counts of error messages per domain
 */
CREATE TABLE responses_twisted (
    hostname VARCHAR(1024),
    twisted_status VARCHAR(16),
    num INTEGER,
    PRIMARY KEY (hostname,twisted_status)
);

CREATE TRIGGER responses_twisted_trigger AFTER INSERT ON responses
BEGIN
    INSERT OR IGNORE INTO responses_twisted
        (hostname,twisted_status,num)
        select hostname,new.twisted_status,0
            from urls 
            inner join frontier on urls.id_urls=frontier.id_urls 
            inner join responses on responses.id_frontier=frontier.id_frontier 
            where responses.id_responses=new.id_responses;
    UPDATE responses_twisted
        SET num=num+1
        where
            twisted_status=new.twisted_status and
            hostname in (select hostname
                from urls 
                inner join frontier on urls.id_urls=frontier.id_urls 
                inner join responses on responses.id_frontier=frontier.id_frontier 
                where responses.id_responses=new.id_responses);
END;

/* this very complicated view extracts only the 2 top subdomains from a host;
 * usually this corresponds to the hostname from the seed file, but sometimes
 * (e.g. for .com.mx hosts) it merges too many hosts together. */
CREATE VIEW responses_twisted_seed AS
    select
    replace(
        hostname,
        rtrim(
            substr(
                rtrim(hostname,replace(hostname,'.','')),
                0,
                length(rtrim(hostname,replace(hostname,'.','')))
            ),
            replace(
                substr(
                    rtrim(hostname,replace(hostname,'.','')),
                    0,
                    length(rtrim(hostname,replace(hostname,'.','')))
                ),
                '.',
                ''
            )
        ),
        ''
    ) as hostname_seed,twisted_status,sum(num) as num
    from responses_twisted
    group by hostname_seed,twisted_status;

/*
 * This cache table stores the total counts of the http codes per domain
 */
CREATE TABLE responses_http (
    hostname VARCHAR(1024),
    http_status VARCHAR(16),
    num INTEGER,
    PRIMARY KEY (hostname,http_status)
);

CREATE TRIGGER responses_http_trigger AFTER UPDATE ON responses
BEGIN
    INSERT OR IGNORE INTO responses_http
        (hostname,http_status,num)
        select hostname,new.http_status,0
            from urls 
            inner join frontier on urls.id_urls=frontier.id_urls 
            inner join responses on responses.id_frontier=frontier.id_frontier 
            where responses.id_responses=new.id_responses;
    UPDATE responses_http
        SET num=num+1
        where
            http_status=new.http_status and
            hostname in (select hostname
                from urls 
                inner join frontier on urls.id_urls=frontier.id_urls 
                inner join responses on responses.id_frontier=frontier.id_frontier 
                where responses.id_responses=new.id_responses);
END;

/*
 * This cache table stores the total number of bytes in the downloaded files 
 * from each domain
 */
CREATE TABLE responses_bytes (
    hostname VARCHAR(1024),
    bytes INTEGER,
    num INTEGER,
    PRIMARY KEY (hostname)
);

CREATE TRIGGER responses_bytes_trigger AFTER UPDATE ON responses
BEGIN
    INSERT OR IGNORE INTO responses_bytes
        (hostname,bytes,num)
        select hostname,0,0
            from urls 
            inner join frontier on urls.id_urls=frontier.id_urls 
            inner join responses on responses.id_frontier=frontier.id_frontier 
            where responses.id_responses=new.id_responses;
    UPDATE responses_bytes
        SET bytes=bytes+new.bytes,
            num=num+1
        where
            hostname in (select hostname
                from urls 
                inner join frontier on urls.id_urls=frontier.id_urls 
                inner join responses on responses.id_frontier=frontier.id_frontier 
                where responses.id_responses=new.id_responses);
END;

