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
    BEGIN
        EXECUTE format('LOCK %s IN EXCLUSIVE MODE', table_to_lock);
        RAISE 'release table lock';
    EXCEPTION WHEN OTHERS THEN
    END;

    /*
     * Remember the end of the window to continue from there next time.
     */
    UPDATE rollups SET last_aggregated_id = window_end-1 WHERE name = rollup_name;
END;
$function$;

CREATE FUNCTION do_rollup(
    name text,
    max_rollup_size bigint default 100000000,
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
    FROM incremental_rollup_window(name,max_rollup_size);

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
 * Rollup table for refs
 */

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
    INNER JOIN urls AS urls_source ON urls_source.id_urls = refs.source
    INNER JOIN urls AS urls_target ON urls_target.id_urls = refs.target
    WHERE
        refs.id_refs >= $1 AND 
        refs.id_refs < $2 
    GROUP BY year_source,urls_source.hostname,urls_target.hostname,type
    ON CONFLICT (year,hostname_source,hostname_target,type)
    DO UPDATE SET num = refs_hostname.num+excluded.num
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

CREATE VIEW total_bytes AS
    SELECT pg_size_pretty(sum(bytes)) FROM responses_summary;
