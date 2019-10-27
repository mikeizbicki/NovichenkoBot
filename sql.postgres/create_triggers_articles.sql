/*
 * This table caches the number of articles are found on each hostname/year
 * combination, with separate fields for the size of articles.
 * FIXME: add keywords support.
 */
CREATE TABLE IF NOT EXISTS articles_hostnames (
    hostname VARCHAR(2048),
    year INTEGER DEFAULT 0 NOT NULL,
    num_articles INTEGER,
    num_100 INTEGER,
    num_500 INTEGER,
    num_1000 INTEGER,
    PRIMARY KEY (hostname,year)
);

CREATE TRIGGER IF NOT EXISTS articles_hostnames_trigger AFTER INSERT ON articles
BEGIN
    INSERT OR IGNORE INTO articles_hostnames 
        (hostname,year,num_articles,num_100,num_500,num_1000) 
        SELECT
            hostname,
            CASE WHEN strftime('%Y',new.pub_time) NOT NULL THEN strftime('%Y',new.pub_time) ELSE 'unk' END ,
            0,0,0,0
        FROM
            urls where urls.id_urls=new.id_urls;

    UPDATE articles_hostnames 
        SET num_articles=num_articles+1, 
            num_100 =CASE WHEN length(new.text)>100  THEN num_100 +1 ELSE num_100  END,
            num_500 =CASE WHEN length(new.text)>500  THEN num_500 +1 ELSE num_500  END,
            num_1000=CASE WHEN length(new.text)>1000 THEN num_1000+1 ELSE num_1000 END
        WHERE 
            hostname in (select hostname from urls where urls.id_urls=new.id_urls) AND 
            year=CASE WHEN strftime('%Y',new.pub_time) NOT NULL THEN strftime('%Y',new.pub_time) ELSE 'unk' END;
END;

/*
 * This table caches the total number of links between different hostnames.
 */

CREATE TABLE IF NOT EXISTS refs_hostnames (
    source_hostname VARCHAR(255),
    target_hostname VARCHAR(255) DEFAULT source_hostname NOT NULL,
    type VARCHAR(10),
    num_refs INTEGER,
    PRIMARY KEY (source_hostname,target_hostname,type)
);

CREATE TRIGGER IF NOT EXISTS refs_hostnames_trigger AFTER INSERT ON refs --WHEN new.type='link'
BEGIN
    INSERT OR IGNORE INTO refs_hostnames 
        (source_hostname,target_hostname,type,num_refs) 
        select source_hostname,target_hostname,type,0
        from refs_urls where refs_urls.id_refs=new.id_refs;
    UPDATE refs_hostnames 
    SET num_refs=num_refs+1 
    WHERE 
        source_hostname in (select source_hostname from refs_urls where refs_urls.id_refs=new.id_refs) and 
        target_hostname in (select target_hostname from refs_urls where refs_urls.id_refs=new.id_refs);
END;

