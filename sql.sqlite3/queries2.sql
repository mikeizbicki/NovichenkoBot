explain query plan 
select keywords.*,scheme,hostname,path
from frontier
inner join urls on urls.id_urls=frontier.id_urls
inner join articles_refs on articles_refs.target=frontier.id_urls
inner join articles on articles.id_urls=articles_refs.source
inner join keywords on articles.id_articles=keywords.id_articles
where
    frontier.timestamp_processed is null 
--group by
    --hostname
order by
    num_alltext desc
    --timestamp_received desc
limit 10;

    num_text desc,
    num_alltext desc,
    timestamp_received desc

CREATE INDEX urls_hostname ON urls(hostname);

CREATE INDEX keywords_title ON keywords(num_title);
CREATE INDEX keywords_text ON keywords(num_text);
CREATE INDEX keywords_alltext ON keywords(num_alltext);
CREATE INDEX keywords_num ON keywords(num_title,num_text,num_alltext);

CREATE INDEX frontier_timestamp_received ON frontier(timestamp_received);
CREATE INDEX frontier_timestamp_processed ON frontier(timestamp_processed);

explain query plan
select frontier.id_urls
from urls 
inner join frontier
on urls.id_urls=frontier.id_urls
where
    urls.hostname = 'cnn.com';

explain query plan
select scheme,hostname,path
from urls
inner join frontier 
    on urls.id_urls=frontier.id_urls
where
    urls.hostname='cnn.com';
