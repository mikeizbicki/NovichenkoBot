-- Summary tables/triggers below for faster data analysis

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
        VALUES 
        (new.hostname,CASE WHEN strftime('%Y',new.pub_time) NOT NULL THEN strftime('%Y',new.pub_time) ELSE 0 END,0,0,0,0);
    UPDATE articles_hostnames 
        SET num_articles=num_articles+1, 
            num_100 =CASE WHEN length(new.text)>100  THEN num_100 +1 ELSE num_100  END,
            num_500 =CASE WHEN length(new.text)>500  THEN num_500 +1 ELSE num_500  END,
            num_1000=CASE WHEN length(new.text)>1000 THEN num_1000+1 ELSE num_1000 END
        WHERE hostname=new.hostname AND year=CASE WHEN strftime('%Y',new.pub_time) NOT NULL THEN strftime('%Y',new.pub_time) ELSE 0 END;
END;

CREATE TABLE IF NOT EXISTS refs_hostnames (
    hostname_source VARCHAR(2048),
    hostname_target VARCHAR(2048) DEFAULT hostname_source NOT NULL,
    type VARCHAR(10),
    num_refs INTEGER,
    PRIMARY KEY (hostname_source,hostname_target,type)
);

CREATE TRIGGER IF NOT EXISTS refs_hostnames_trigger AFTER INSERT ON refs --WHEN new.type='link'
BEGIN
    INSERT OR IGNORE INTO refs_hostnames 
        (hostname_source,hostname_target,type,num_refs) 
        VALUES 
        (new.hostname_source,CASE WHEN new.hostname_target='' THEN new.hostname_source ELSE new.hostname_target END,new.type,0);
    UPDATE refs_hostnames SET num_refs=num_refs+1 
        WHERE hostname_source=new.hostname_source AND hostname_target=CASE WHEN new.hostname_target='' THEN new.hostname_source ELSE new.hostname_target END AND type=new.type;
END;

CREATE TABLE IF NOT EXISTS refs_urls (
    hostname_source VARCHAR(2048),
    hostname_target VARCHAR(2048),
    type VARCHAR(10),
    url VARCHAR(2048),
    num_urls INTEGER,
    PRIMARY KEY (hostname_source,type,url)
);

CREATE TRIGGER IF NOT EXISTS refs_urls_trigger AFTER INSERT ON refs --WHEN new.type='link'
BEGIN
    INSERT OR IGNORE INTO refs_urls
        (hostname_source,hostname_target,type,url,num_urls) 
        VALUES 
        (new.hostname_source,CASE WHEN new.hostname_target='' THEN new.hostname_source ELSE new.hostname_target END,new.type,new.url,0);
    UPDATE refs_urls SET num_urls=num_urls+1 
        WHERE hostname_source=new.hostname_source AND type=new.type AND url=new.url;
END;

