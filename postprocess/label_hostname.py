#! /usr/bin/python3

# command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',type=str,default='postgres:///novichenkobot')
parser.add_argument('--hostname',type=str,required=True)
args = parser.parse_args()

# import libraries
import sqlalchemy
import datetime
from textblob import TextBlob

# database connection
print('connect to db')
engine = sqlalchemy.create_engine(args.db, connect_args={'connect_timeout': 120})
connection = engine.connect()

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_sentences,get_label


########################################
# FIXME: should these label functions find a better home?

def polarity(text):
    return TextBlob(text).sentiment.polarity

def subjectivity(text):
    return TextBlob(text).sentiment.subjectivity

import textstat
label_funcs=[
    textstat.syllable_count,
    textstat.lexicon_count,
    textstat.flesch_reading_ease,
    textstat.smog_index,
    textstat.flesch_kincaid_grade,
    textstat.coleman_liau_index,
    textstat.automated_readability_index,
    textstat.dale_chall_readability_score,
    textstat.difficult_words,
    textstat.linsear_write_formula,
    textstat.gunning_fog,
    polarity,
    subjectivity
    ]

def label_hostname(connection,hostname):

    # print summary stats of articles from hostname
    sql=sqlalchemy.sql.text('''
        SELECT count(1)
        FROM articles
        WHERE
            hostname=:hostname;
    ''')
    total_articles=connection.execute(sql,{
        'hostname':hostname
        }).first()[0]
    print(f'total_articles = {total_articles}')

    sql=sqlalchemy.sql.text('''
        SELECT count(1)
        FROM get_valid_articles(:hostname);
    ''')
    total_unique=connection.execute(sql,{
        'hostname':hostname
        }).first()[0]
    print(f'total_unique = {total_unique}')
    print(f'total_unique / total_articles = {total_unique / float(total_articles)}')

    sql=sqlalchemy.sql.text('''
        SELECT count(1)
        FROM get_valid_articles(:hostname)
        WHERE id_articles NOT IN (SELECT id_articles FROM sentences);
    ''')
    total_unprocessed=connection.execute(sql,{
        'hostname':hostname
        }).first()[0]
    print(f'total_unprocessed = {total_unprocessed}')
    print(f'total_unprocessed / total_unique = {total_unprocessed / float(total_unique)}')


    # loop through each unique article to label it
    i=-1
    while True:
        i+=1
        print(datetime.datetime.now(),f': i={i}')

        # get next article from hostname
        sql=sqlalchemy.sql.text('''
            SELECT id_articles,text,title
            FROM get_valid_articles(:hostname)
            WHERE id_articles NOT IN (SELECT id_articles FROM sentences)
            LIMIT 1;
        ''')
        res=connection.execute(sql,{
            'hostname':hostname
            }).first()

        # if no results, then terminate the loop
        if res is None:
            print(f'ERROR: no rows found for {hostname}')
            return i

        # otherwise, create the labels
        id_articles,text,title = res
        sentences = get_sentences(connection,id_articles,text=text,title=title)
        for id_sentences,sentence in enumerate(sentences):
            for label_func in label_funcs:
                get_label(connection,id_articles,id_sentences,label_func,sentence=sentence)

label_hostname(connection,args.hostname)
