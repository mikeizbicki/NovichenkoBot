#!/usr/bin/python3

# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--inputs',nargs='+',required=True)
parser.add_argument('--min_id',default=-1,type=int)
parser.add_argument('--print_every',type=int,default=100)
args = parser.parse_args()

# imports
import sqlalchemy
import os
import datetime
import zipfile
import io
import simplejson as json
import reverse_geocoder as rg

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from Novichenko.Bot.sqlalchemy_utils import get_url_info

# create database connection
engine = sqlalchemy.create_engine(args.db, connect_args={
    'application_name': 'Novichenko.Tweets.load_tweets',
    })
connection = engine.connect()

# helper functions
def remove_nulls(s):
    '''
    Postgres doesn't support strings with the null character \x00 in them,
    but twitter does.
    This helper function replaces the null characters with an escaped version so that they can be loaded into postgres.
    Technically, this means the data in postgres won't be an exact match of the data in twitter,
    because we are not also escaping the escaped strings.
    The added complexity of a fulling working escaping system doesn't seem worth the benefits since null characters appear so rarely in twitter text.
    '''
    if s is None:
        return None
    else:
        return s.replace('\x00','\\x00')

def insert_tweet(connection,tweet):
    '''
    Inserts the tweet into the database.

    Args:
        connection: a sqlalchemy connection to the postgresql db
        tweet: a dictionary representing the json tweet object
    '''
    # skip tweet if already inserted
    sql=sqlalchemy.sql.text('''
    SELECT id_tweets 
    FROM twitter.tweets
    WHERE id_tweets = :id_tweets
        ''')
    res = connection.execute(sql,{
        'id_tweets':tweet['id'],
        })
    if res.first() is not None:
        return

    # insert tweet
    with connection.begin() as trans:

        ########################################
        if tweet['user']['url'] is None:
            tweet_user_url = {}
        else:
            tweet_user_url = get_url_info(connection,tweet['user']['url'])


        # FIXME: ON CONFLICT should update the user if this is from a more recent tweet
        sql=sqlalchemy.sql.text('''
        INSERT INTO twitter.users
            (id_users,created_at,updated_at,screen_name,name,location,id_urls,hostname,description,protected,verified,friends_count,listed_count,favourites_count,statuses_count,withheld_in_countries)
            VALUES
            (:id_users,:created_at,:updated_at,:screen_name,:name,:location,:id_urls,:hostname,:description,:protected,:verified,:friends_count,:listed_count,:favourites_count,:statuses_count,:withheld_in_countries)
            ON CONFLICT (id_users) DO
            UPDATE SET
                created_at=:created_at,
                updated_at=:updated_at,
                screen_name=:screen_name,
                name=:name,
                location=:location,
                id_urls=:id_urls,
                hostname=:hostname,
                description=:description,
                protected=:protected,
                verified=:verified,
                friends_count=:friends_count,
                listed_count=:listed_count,
                favourites_count=:favourites_count,
                statuses_count=:statuses_count,
                withheld_in_countries=:withheld_in_countries
                WHERE :updated_at > twitter.users.updated_at OR twitter.users.updated_at is null

            ''')
        res = connection.execute(sql,{
            'id_users':tweet['user']['id'],
            'created_at':tweet['user']['created_at'],
            'updated_at':tweet['created_at'],
            'screen_name':remove_nulls(tweet['user']['screen_name']),
            'name':remove_nulls(tweet['user']['name']),
            'location':remove_nulls(tweet['user']['location']),
            'id_urls':tweet_user_url.get('id_urls',None),
            'hostname':tweet_user_url.get('hostname',None),
            'description':remove_nulls(tweet['user']['description']),
            'protected':tweet['user']['protected'],
            'verified':tweet['user']['verified'],
            'friends_count':tweet['user']['friends_count'],
            'listed_count':tweet['user']['listed_count'],
            'favourites_count':tweet['user']['favourites_count'],
            'statuses_count':tweet['user']['statuses_count'],
            'withheld_in_countries':tweet['user'].get('withheld_in_countries',None),
            })

        ########################################

        try:
            #geo = [[ (tweet['geo']['coordinates'][1],tweet['geo']['coordinates'][0]) ]]
            geo_coords = tweet['geo']['coordinates']
            geo_coords = str(tweet['geo']['coordinates'][0]) + ' ' + str(tweet['geo']['coordinates'][1])
            geo_str = 'POINT'
        except TypeError:
            try:
                geo_coords = '('
                for i,poly in enumerate(tweet['place']['bounding_box']['coordinates']):
                    if i>0:
                        geo_coords+=','
                    geo_coords+='('
                    for j,point in enumerate(poly):
                        geo_coords+= str(point[0]) + ' ' + str(point[1]) + ','
                    geo_coords+= str(poly[0][0]) + ' ' + str(poly[0][1])
                    geo_coords+=')'
                geo_coords+=')'
                geo_str = 'MULTIPOLYGON'
            except KeyError:
                if tweet['user']['geo_enabled']:
                    raise ValueError('couldnt find geolocation information')

        try:
            text = tweet['extended_tweet']['full_text']
        except:
            text = tweet['text']

        try:
            country_code = tweet['place']['country_code'].lower()
        except TypeError:
            country_code = None

        if country_code == 'us':
            state_code = tweet['place']['full_name'].split(',')[-1].strip().lower()
            if len(state_code)>2:
                state_code = None
        else:
            state_code = None

        try:
            place_name = tweet['place']['full_name']
        except TypeError:
            place_name = None

        # created unhydrated references
        if tweet.get('in_reply_to_user_id',None) is not None:
            sql=sqlalchemy.sql.text('''
            INSERT INTO twitter.users
                (id_users,screen_name)
                VALUES
                (:id_users,:screen_name)
            ON CONFLICT DO NOTHING
                ''')
            res = connection.execute(sql,{
                'id_users':tweet['in_reply_to_user_id'],
                'screen_name':tweet['in_reply_to_screen_name'],
                })

        # insert the tweet
        sql=sqlalchemy.sql.text(f'''
        INSERT INTO twitter.tweets
            (id_tweets,id_users,created_at,in_reply_to_status_id,in_reply_to_user_id,quoted_status_id,geo,retweet_count,quote_count,favorite_count,withheld_copyright,withheld_in_countries,place_name,country_code,state_code,lang,text,source)
            VALUES
            (:id_tweets,:id_users,:created_at,:in_reply_to_status_id,:in_reply_to_user_id,:quoted_status_id,ST_GeomFromText(:geo_str || '(' || :geo_coords || ')'),:retweet_count,:quote_count,:favorite_count,:withheld_copyright,:withheld_in_countries,:place_name,:country_code,:state_code,:lang,:text,:source)
        ON CONFLICT DO NOTHING;
            ''')
        res = connection.execute(sql,{
            'id_tweets':tweet['id'],
            'id_users':tweet['user']['id'],
            'created_at':tweet['created_at'],
            'in_reply_to_status_id':tweet.get('in_reply_to_status_id',None),
            'in_reply_to_user_id':tweet.get('in_reply_to_user_id',None),
            'quoted_status_id':tweet.get('quoted_status_id',None),
            'geo_coords':geo_coords,
            'geo_str':geo_str,
            'retweet_count':tweet.get('retweet_count',None),
            'quote_count':tweet.get('quote_count',None),
            'favorite_count':tweet.get('favorite_count',None),
            'withheld_copyright':tweet.get('withheld_copyright',None),
            'withheld_in_countries':tweet.get('withheld_in_countries',None),
            'place_name':place_name,
            'country_code':country_code,
            'state_code':state_code,
            'lang':tweet.get('lang'),
            'text':remove_nulls(text),
            'source':remove_nulls(tweet.get('source',None)),
            })

        ########################################

        try:
            urls = tweet['extended_tweet']['entities']['urls']
        except KeyError:
            urls = tweet['entities']['urls']

        for url in urls:
            url_info = get_url_info(connection,url['expanded_url'])
            if url_info is None:
                id_urls = None
                hostname = None
            else:
                id_urls = url_info['id_urls']
                hostname = url_info['hostname']
            sql=sqlalchemy.sql.text('''
            INSERT INTO twitter.tweet_urls
                (id_tweets,id_urls,hostname)
                VALUES
                (:id_tweets,:id_urls,:hostname)
            ON CONFLICT DO NOTHING
                ''')
            res = connection.execute(sql,{
                'id_tweets':tweet['id'],
                'id_urls':id_urls,
                'hostname':hostname
                })

        ########################################

        try:
            mentions = tweet['extended_tweet']['entities']['user_mentions']
        except KeyError:
            mentions = tweet['entities']['user_mentions']

        for mention in mentions:
            sql=sqlalchemy.sql.text('''
            INSERT INTO twitter.users
                (id_users,name,screen_name)
                VALUES
                (:id_users,:name,:screen_name)
            ON CONFLICT DO NOTHING
                ''')
            res = connection.execute(sql,{
                'id_users':mention['id'],
                'name':remove_nulls(mention['name']),
                'screen_name':remove_nulls(mention['screen_name']),
                })

            sql=sqlalchemy.sql.text('''
            INSERT INTO twitter.tweet_mentions
                (id_tweets,id_users)
                VALUES
                (:id_tweets,:id_users)
            ON CONFLICT DO NOTHING
                ''')
            res = connection.execute(sql,{
                'id_tweets':tweet['id'],
                'id_users':mention['id']
                })

        ########################################

        try:
            hashtags = tweet['extended_tweet']['entities']['hashtags'] 
            cashtags = tweet['extended_tweet']['entities']['symbols'] 
        except KeyError:
            hashtags = tweet['entities']['hashtags']
            cashtags = tweet['entities']['symbols']

        tags = [ '#'+hashtag['text'] for hashtag in hashtags ] + [ '$'+cashtag['text'] for cashtag in cashtags ]

        for tag in tags:
            sql=sqlalchemy.sql.text('''
            INSERT INTO twitter.tweet_tags
                (id_tweets,tag)
                VALUES
                (:id_tweets,:tag)
            ON CONFLICT DO NOTHING
                ''')
            res = connection.execute(sql,{
                'id_tweets':tweet['id'],
                'tag':remove_nulls(tag)
                })

        ########################################

        try:
            media = tweet['extended_tweet']['extended_entities']['media']
        except KeyError:
            try:
                media = tweet['extended_entities']['media']
            except KeyError:
                media = []

        for medium in media:
            url_info = get_url_info(connection,medium['media_url'])
            sql=sqlalchemy.sql.text('''
            INSERT INTO twitter.tweet_media
                (id_tweets,id_urls,hostname,type)
                VALUES
                (:id_tweets,:id_urls,:hostname,:type)
            ON CONFLICT DO NOTHING
                ''')
            res = connection.execute(sql,{
                'id_tweets':tweet['id'],
                'id_urls':url_info['id_urls'],
                'hostname':url_info['hostname'],
                'type':medium['type']
                })

# loop through file
# NOTE:
# we reverse sort the filenames because this results in fewer updates to the users table,
# which prevents excessive dead tuples and autovacuums
for filename in sorted(args.inputs, reverse=True):
    with zipfile.ZipFile(filename, 'r') as archive: 
        #with connection.begin() as trans:
            print(datetime.datetime.now(),filename)
            for subfilename in sorted(archive.namelist(), reverse=True):
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    tweets_list = []
                    for i,line in enumerate(f):
                        tweet = json.loads(line)

                        # print message
                        #tweets_list.append(tweet)
                        if i%args.print_every==0:
                            print(datetime.datetime.now(),filename,subfilename,'i=',i,'id=',tweet['id'])
                            #with connection.begin() as trans:
                                #for tweet in tweets_list:
                                    #insert_tweet(connection,tweet)
                            #tweets_list = []
                        insert_tweet(connection,tweet)

                        """

                        # skip tweets before threshold
                        if tweet['id']<args.min_id:
                            continue

                        # insert tweets
                        insert_tweet(connection,tweet)
                        """
