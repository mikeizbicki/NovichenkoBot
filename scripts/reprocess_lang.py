'''
Many of the non-english languages has a bug where keywords were not properly detected.
Some example languages include ru,zh,ja, and kr, which are all very important.
This file is a one-off script for reprocessing articles in these languages to detect the keywords.
There is a max(id_articles) constraint because the general_spider should be fixed for articles after this.

There are two known issues with this script:

1) Instead of updating the existing keywords entries, we add new entries into the table.
This causes duplicates in the keywords table for different articles.
This was necessary to prevent having to reaggregate refs_summary from scratch.
This will cause slightly inflated counts in the integer fields of refs_summary,
but shouldn't affect the hll fields (which are more important).

2) To improve speed, the script does not consider the original html,
and thus the keywords_alltext field is undercounted.

3) This script does not update priorities in the frontier for links coming from pages with keywords.

It has currently been run on: ru
'''

# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--lang',required=True)
parser.add_argument('--limit',type=int,default=100)
parser.add_argument('--hostname',type=str)
parser.add_argument('--dryrun',action='store_true')
parser.add_argument('--id_articles_max',type=int,default=136050074)
parser.add_argument('--id_articles_min',type=int,default=0)
parser.add_argument('--keywords',default='inputs/keywords.txt')
args = parser.parse_args()

# imports
import sqlalchemy
import os
import datetime
import itertools

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_url_info,insert_request

# create database connection
engine = sqlalchemy.create_engine(args.db)
connection = engine.connect()

# load keywords dictionaries
keywords={}
with open(args.keywords) as f:
    for line in f:
        lang,wordstr=line.split(':')
        words=[word.strip().lower() for word in wordstr.split(',')]
        keywords[lang]=words
keywords_lang=keywords.get(args.lang,[])

# sql clauses
where_hostname = ''
if args.hostname is not None:
    where_hostname = 'hostname = :hostname and'

# loop through articles
for i in itertools.count(0):
    print(datetime.datetime.now(),f'i={i} id_articles_min={args.id_articles_min}')
    sql = sqlalchemy.sql.text(f'''
    SELECT articles.id_articles,title,text 
    FROM articles
    WHERE 
        {where_hostname}
        id_articles<:id_articles_max and
        id_articles>:id_articles_min and
        lang=:lang
    ORDER BY id_articles ASC
    LIMIT :limit;
    ''');
    res = connection.execute(sql, {
        'hostname':args.hostname,
        'lang':args.lang,
        'offset':i*args.limit,
        'limit':args.limit,
        'id_articles_max':args.id_articles_max,
        'id_articles_min':args.id_articles_min,
        })
    rows = list(res)

    # update keywords table
    with connection.begin() as trans:
        for row in rows:
            args.id_articles_min = max(args.id_articles_min, row['id_articles'])

            text_lower=row['text'].lower()
            title_lower=row['title'].lower()
            keywords_text=sum([ text_lower.count(keyword)       for keyword in keywords_lang])
            keywords_title=sum([ title_lower.count(keyword)     for keyword in keywords_lang])
            keywords_alltext = keywords_text+keywords_title

            # only add a new row if there are keywords
            if keywords_text>0 or keywords_title>0:
                print(f'  id_articles={row["id_articles"]},keywords_text={keywords_text},keywords_title={keywords_title}')
                if not args.dryrun:
                    sql=sqlalchemy.sql.text('''
                    INSERT INTO keywords
                        (id_articles,keyword,num_title,num_text,num_alltext)
                        VALUES
                        (:id_articles,:keyword,:num_title,:num_text,:num_alltext)
                    ''')
                    res=connection.execute(sql,{
                        'id_articles':row['id_articles'],
                        'keyword':'north korea',
                        'num_title':keywords_title,
                        'num_text':keywords_text,
                        'num_alltext':keywords_alltext,
                        })

    if len(rows)==0:
        break
