import sqlalchemy

engine = sqlalchemy.create_engine('postgres:///novichenkobot', connect_args={
    'application_name': 'example',
    })
connection = engine.connect()

sql=sqlalchemy.sql.text('''
SELECT
    date_trunc('day',created_at) as day,
    count(*) as count
FROM twitter.tweet_tags
INNER JOIN twitter.tweets ON tweet_tags.id_tweets = tweets.id_tweets
WHERE
    tag = :tag
GROUP BY day
ORDER BY day DESC;
    ''')
res = connection.execute(sql,{
    'tag':'#coronavirus',
    })

for row in res:
    print(row['day'], row['count'])
