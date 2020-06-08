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
CREATE INDEX tweet_mentions_index ON twitter.tweet_mentions(id_users);
CREATE UNIQUE INDEX tweet_mentions_unique ON twitter.tweet_mentions(id_users,id_tweets);

CREATE TABLE twitter.tweet_tags (
    id_tweets BIGINT,
    tag TEXT,
    FOREIGN KEY (id_tweets) REFERENCES twitter.tweets(id_tweets)
);
COMMENT ON TABLE twitter.tweet_tags IS 'This table links both hashtags and cashtags';
CREATE INDEX tweet_tags_index ON twitter.tweet_tags(id_tweets);
CREATE UNIQUE INDEX tweet_tags_unique ON twitter.tweet_tags(tag,id_tweets);
--CREATE INDEX tweet_tags_unique2 ON twitter.tweet_tags(lower(tag),id_tweets);

/*
CREATE TABLE twitter.tweet_tags2 (
    id_tweets BIGINT,
    tag TEXT,
    FOREIGN KEY (id_tweets) REFERENCES twitter.tweets(id_tweets)
);
COMMENT ON TABLE twitter.tweet_tags2 IS 'This table links both hashtags and cashtags';
CREATE INDEX tweet_tags2_index ON twitter.tweet_tags2(id_tweets);
--CREATE INDEX tweet_tags2_index2 ON twitter.tweet_tags2(tag);
--CREATE INDEX tweet_tags2_index3 ON twitter.tweet_tags2(lower(tag));
CREATE UNIQUE INDEX tweet_tags2_unique ON twitter.tweet_tags2(tag,id_tweets);
*/

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
 *
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
*/

/*
 * Calculates the hashtags that are commonly used with another hashtag
 */
SELECT lower(t1.tag) as tag,count(*) as count
FROM twitter.tweet_tags t1
INNER JOIN twitter.tweet_tags t2 ON t1.id_tweets = t2.id_tweets
WHERE
    lower(t2.tag)='#coronavirus'
GROUP BY (1)
ORDER BY count DESC;

/*
 * Like the above query, 
 * but also returns the total number of times each hashtag is used
 */
SELECT t1.tag, shared_count, total_count
FROM (
    SELECT lower(t1.tag) as tag,count(*) as shared_count
    FROM twitter.tweet_tags t1
    INNER JOIN twitter.tweet_tags t2 ON t1.id_tweets = t2.id_tweets
    WHERE
        lower(t2.tag)='#coronavirus'
    GROUP BY (1)
    ORDER BY shared_count DESC
    LIMIT 100
) t1
INNER JOIN (
    SELECT 
        lower(tag) as tag,
        count(*) as total_count
        FROM twitter.tweet_tags
        GROUP BY (1)
    ) t3 ON t1.tag = t3.tag
ORDER BY shared_count DESC
;

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
GROUP BY country_code, state_code
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

/*
 * Emoji tables are not provided by the twitter API but calculated within python
 */
CREATE TABLE twitter.tweet_emoji (
    id_tweets BIGINT,
    id_emoji INTEGER NOT NULL,
    num SMALLINT NOT NULL,
    FOREIGN KEY (id_tweets) REFERENCES twitter.tweets(id_tweets),
    FOREIGN KEY (id_emoji) REFERENCES twitter.emoji(id_emoji)
);
CREATE INDEX tweet_emoji_index ON twitter.tweet_emoji(id_emoji);
CREATE UNIQUE INDEX tweet_emoji_unique ON twitter.tweet_emoji(id_emoji,id_tweets);

CREATE TABLE twitter.emoji (
    id_emoji SERIAL PRIMARY KEY,
    emoji TEXT NOT NULL,
    name TEXT NOT NULL
);
CREATE UNIQUE INDEX emoji_unique1 ON twitter.emoji(id_emoji);
CREATE UNIQUE INDEX emoji_unique2 ON twitter.emoji(emoji);

/*
 * Sentiment is calculated according to the nvidia-sentiment library
 */
CREATE TABLE twitter.tweet_sentiment (
    id_tweets BIGINT PRIMARY KEY,
    anger REAL NOT NULL,
    anticipation REAL NOT NULL,
    disgust REAL NOT NULL,
    fear REAL NOT NULL,
    joy REAL NOT NULL,
    sadness REAL NOT NULL,
    surprise REAL NOT NULL,
    trust REAL NOT NULL,
    model TEXT NOT NULL,
    FOREIGN KEY (id_tweets) REFERENCES twitter.tweets(id_tweets)
);

/*
 * Stefanos:
 * 1. Use the SELECT query below to get the next set of tweets
 * 2. Use the function you created to get the sentiment of these tweets
 * 3. Use the INSERT query below to insert them into the database
 */
SELECT
    tweets.id_tweets,
    text
FROM twitter.tweet_tags
INNER JOIN twitter.tweets ON tweet_tags.id_tweets = tweets.id_tweets
WHERE
    tweets.id_tweets NOT IN (SELECT id_tweets FROM twitter.tweet_sentiment) AND
    lang='en' AND
    tag = '#coronavirus' 
LIMIT 100;

INSERT INTO twitter.tweet_sentiment
    (id_tweets,anger,anticipation,disgust,fear,joy,sadness,surprise,trust,model)
    VALUES
    (1225192375816982528,0,0,0,0,0,0,0,0,'example');
