CREATE EXTENSION postgis;
CREATE SCHEMA twitter;

/*
 * Users may be partially hydrated with only a name/screen_name 
 * if they are first encountered during a quote/reply/mention 
 * inside of a tweet someone else's tweet.
 */
CREATE TABLE twitter.users (
    id_users BIGINT PRIMARY KEY,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    id_urls BIGINT REFERENCES urls(id_urls),
    friends_count INTEGER,
    listed_count INTEGER,
    favourites_count INTEGER,
    statuses_count INTEGER,
    protected BOOLEAN,
    verified BOOLEAN,
    hostname VARCHAR(253),
    screen_name TEXT,
    name TEXT,
    location TEXT,
    description TEXT,
    withheld_in_countries VARCHAR(2)[],
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls)
);

CREATE TABLE twitter.tweets (
    id_tweets BIGINT PRIMARY KEY,
    id_users BIGINT,
    created_at TIMESTAMPTZ,
    in_reply_to_status_id BIGINT,
    in_reply_to_user_id BIGINT,
    quoted_status_id BIGINT,
    retweet_count SMALLINT,
    favorite_count SMALLINT,
    quote_count SMALLINT,
    withheld_copyright BOOLEAN,
    withheld_in_countries VARCHAR(2)[],
    source TEXT,
    text TEXT,
    country_code VARCHAR(2),
    state_code VARCHAR(2),
    lang TEXT,
    place_name TEXT,
    geo geometry,
    FOREIGN KEY (id_users) REFERENCES twitter.users(id_users),
    FOREIGN KEY (in_reply_to_user_id) REFERENCES twitter.users(id_users)

    -- NOTE:
    -- We do not have the following foreign keys because they would require us
    -- to store many unhydrated tweets in this table.
    -- FOREIGN KEY (in_reply_to_status_id) REFERENCES twitter.tweets(id_tweets),
    -- FOREIGN KEY (quoted_status_id) REFERENCES twitter.tweets(id_tweets)
);
COMMENT ON TABLE twitter.tweets IS 'Tweets may be entered in hydrated or unhydrated form.';
--CREATE INDEX tweets_index_pgroonga ON twitter.tweets USING pgroonga(text);
CREATE INDEX tweets_index_geo ON twitter.tweets USING gist(geo);
CREATE INDEX tweets_index_withheldincountries ON twitter.tweets USING gin(withheld_in_countries);

CREATE TABLE twitter.tweet_urls (
    id_tweets BIGINT,
    id_urls BIGINT,
    hostname VARCHAR(253),
    FOREIGN KEY (id_tweets) REFERENCES twitter.tweets(id_tweets),
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls)
);
CREATE INDEX tweet_urls_index ON twitter.tweet_urls(reverse(hostname),id_urls);
CREATE UNIQUE INDEX tweet_urls_unique ON twitter.tweet_urls(id_tweets,id_urls);

CREATE TABLE twitter.tweet_mentions (
    id_tweets BIGINT,
    id_users BIGINT,
    FOREIGN KEY (id_tweets) REFERENCES twitter.tweets(id_tweets),
    FOREIGN KEY (id_users) REFERENCES twitter.users(id_users)
);
--CREATE INDEX tweet_mentions_index ON twitter.tweet_mentions(id_users);
CREATE UNIQUE INDEX tweet_mentions_unique ON twitter.tweet_mentions(id_users,id_tweets);

CREATE TABLE twitter.tweet_tags (
    id_tweets BIGINT,
    tag TEXT,
    FOREIGN KEY (id_tweets) REFERENCES twitter.tweets(id_tweets)
);
COMMENT ON TABLE twitter.tweet_tags IS 'This table links both hashtags and cashtags';
CREATE INDEX tweet_tags_index ON twitter.tweet_tags(id_tweets);
CREATE UNIQUE INDEX tweet_tags_unique ON twitter.tweet_tags(tag,id_tweets);

CREATE TABLE twitter.tweet_media (
    id_tweets BIGINT,
    id_urls BIGINT,
    hostname VARCHAR(253),
    type TEXT,
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls),
    FOREIGN KEY (id_tweets) REFERENCES twitter.tweets(id_tweets)
);
CREATE UNIQUE INDEX tweet_media_unique ON twitter.tweet_media(id_tweets,id_urls);


/*
 * FIXME: should we add these tables?
 *
CREATE TABLE twitter.datasets (
    id_datasets SERIAL PRIMARY KEY,
    name TEXT
);

CREATE TABLE twitter.datasets (
    id_datasets INTEGER,
    id_tweets BIGINT,
    FOREIGN KEY (id_datasets_info) REFERENCES twitter.datasets_info(id_datasets)
    -- NOTE:
    -- We do not have the following foreign key because we may create datasets without having downloaded the tweets
    -- FOREIGN KEY (id_datasets) REFERENCES twitter.datasets_info(id_datasets)
);
*/

/*
 * Precomputes the total number of occurrences for each hashtag
 */
CREATE MATERIALIZED VIEW twitter.tweet_tags_total AS (
    SELECT 
        row_number() over (order by count(*) desc) AS row,
        tag, 
        count(*) AS total
    FROM twitter.tweet_tags
    GROUP BY tag
    ORDER BY total DESC
);

/*
 * Precomputes the number of hashtags that co-occur with each other
 */
CREATE MATERIALIZED VIEW twitter.tweet_tags_cooccurrence AS (
    SELECT 
        t1.tag AS tag1,
        t2.tag AS tag2,
        count(*) AS total
    FROM twitter.tweet_tags t1
    INNER JOIN twitter.tweet_tags t2 ON t1.id_tweets = t2.id_tweets
    GROUP BY t1.tag, t2.tag
    ORDER BY total DESC
);

/*
 * Calculates the hashtags that are commonly used with another hashtag
 */
SELECT t1.tag,count(*) as count
FROM twitter.tweet_tags t1
INNER JOIN twitter.tweet_tags t2 ON t1.id_tweets = t2.id_tweets
WHERE
    t2.tag='#coronavirus'
GROUP BY t1.tag
ORDER BY count DESC;

/*
 * Calculates how commonly a hashtag is used in each country.
 */
SELECT 
    country_code,
    count(*) as count
FROM twitter.tweet_tags
INNER JOIN twitter.tweets ON tweet_tags.id_tweets = tweets.id_tweets
WHERE
    tag = '#coronavirus'
GROUP BY country_code
ORDER BY count DESC;

/*
 * Calculates how commonly a hashtag is used in each US state.
 */
SELECT 
    state_code,
    count(*) as count
FROM twitter.tweet_tags
INNER JOIN twitter.tweets ON tweet_tags.id_tweets = tweets.id_tweets
WHERE
    country_code = 'us' AND
    tag = '#coronavirus'
GROUP BY country_code
ORDER BY count DESC;

/*
 * Calculates how commonly a hashtag is used each day.
 */
SELECT
    date_trunc('day',created_at) as day,
    count(*) as count
FROM twitter.tweet_tags
INNER JOIN twitter.tweets ON tweet_tags.id_tweets = tweets.id_tweets
WHERE
    tag = '#coronavirus'
GROUP BY day
ORDER BY day DESC;

/*
 * Selects the text of tweets that are written in english and contain a particular hashtag
 */
SELECT
    tweets.id_tweets,
    text
FROM twitter.tweet_tags
INNER JOIN twitter.tweets ON tweet_tags.id_tweets = tweets.id_tweets
WHERE
    lang='en' AND
    tag = '#coronavirus' 
LIMIT 100;
