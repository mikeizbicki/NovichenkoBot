import sqlalchemy
import datetime
#from scrapy.http import Request,HTMLResponse,Response

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_url_info,reverse_hostname
from NovichenkoBot.spiders.general_spider import GeneralSpider


########################################
# FIXME: should these label functions find a better home?

from textblob import TextBlob

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
    #textstat.text_standard,
    polarity,
    subjectivity
    ]

def label_hostname(connection,hostname):

    # get next article from hostname
    sql=sqlalchemy.sql.text('''
        SELECT DISTINCT ON (title) * FROM (
            SELECT DISTINCT ON (text) id_articles,text,title
            FROM articles
            WHERE 
                hostname=:hostname AND
                pub_time is not null AND
                text is not null AND
                title is not null AND
                id_articles NOT IN (SELECT id_articles FROM SENTENCES)
            LIMIT 1
        ) AS t;
    ''')
    res=connection.execute(sql,{
        'hostname':hostname
        })
    id_articles,text,title=res.first()
    sentences=get_sentences(connection,id_articles,text=text,title=title)

    # do labeling
    for id_sentences,sentence in enumerate(sentences):
        for label_func in label_funcs:
            get_label(connection,id_articles,id_sentences,label_func,sentence=sentence)


################################################################################
# temporary testing stuff goes here

# database connection
print('connect to db')
db='postgres:///novichenkobot'
engine = sqlalchemy.create_engine(db, connect_args={'connect_timeout': 120})
connection = engine.connect()

print('do the stuff')
with open('inputs/fake-news/week2/hacking-1.urls') as f:
    urls=[]
    for url in f.readlines():
        url=url.strip()
        urls.append(url)
        #id_article=url2article(connection,url)
        #print('url=',url)
    id_articles_list=urls2articles(connection,urls)
asd
#test_url='https://www.nknews.org/2012/12/the-top-ten-most-bizarre-rumours-to-spread-about-north-korea/'
#id_articles=url2article(connection,test_url)
#sentences=get_sentences(connection,id_articles)
#score=get_label(connection,id_articles,0,textstat.flesch_reading_ease)
#print('score=',score)

#import datetime
#i=0
#while True:
    #print(datetime.datetime.now(),f': i={i}')
    #label_hostname(connection,'www.northkoreatech.org')
    #i+=1
