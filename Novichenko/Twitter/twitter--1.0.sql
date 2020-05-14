CREATE TABLE twitter (
    data JSONB
);

CREATE UNIQUE INDEX twitter_index_id ON twitter(cast(data->>'id' as bigint));
CREATE INDEX twitter_index_userid ON twitter(cast(data->'user'->>'id' as bigint));
CREATE INDEX twitter_pgroonga ON twitter USING pgroonga(get_tweet(data));

CREATE TABLE twitter_null (
    data JSON
);

CREATE TABLE tweets (
    id_tweets BIGINT,
    userid BIGINT,
    created_at TIMESTAMP,
    text TEXT,
    lang VARCHAR(2),
    retweet_count SMALLINT,
    favorite_count SMALLINT,
    geo POLYGON
);

/*
 * For tweets < 140 characters, the Twitter JSONB object stores the tweet text in data->>'text'.
 * This was the original limit of tweet lengths, 
 * but twitter expanded the length of tweets to be up to 280 characters.
 * Tweets that use this new length are called extended tweets, 
 * and the tweet text is stored in data->'extended_tweet'->>'full_text'.
 * This function provides a convenient wrapper for extracting the text from the appropriate location.
 */
CREATE FUNCTION get_tweet(data JSONB)
RETURNS TEXT AS
$$
BEGIN
    RETURN COALESCE(data->'extended_tweet'->>'full_text', data->>'text');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

/*
CREATE TABLE twitter_urls (
    id_twitter BIGINT NOT NULL,
    id_urls BIGINT NOT NULL,
    hostname VARCHAR(253) NOT NULL,
    -- NOTE: We would like to add the following foreign key, but postgres doesn't support it
    -- FOREIGN KEY (id_twitter) REFERENCES twitter((cast(data->>'id' as bigint))
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED
);
*/
