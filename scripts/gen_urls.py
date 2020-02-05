#!/bin/python3

# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--tld',required=True)
parser.add_argument('--outfile',)
parser.add_argument('--max_hostnames',type=int,default=100)
parser.add_argument('--threshold',type=float,default=0.1)
args = parser.parse_args()

if args.outfile is None:
    args.outfile = args.tld+'.csv'

# imports
import sqlalchemy
import os
import datetime
import csv

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_id_hostnames

# create database connection
engine = sqlalchemy.create_engine(args.db)
connection = engine.connect()

sql=sqlalchemy.sql.text(f'''
    SELECT hostname,all_keywords,all_total,abs(valid_fraction-0.5)/(1+all_keywords)
    FROM hostname_productivity 
    WHERE hostname like '%.{args.tld}'
    ORDER BY abs(valid_fraction-0.5123)/(1+all_keywords) ASC
    limit 100;
    ''')
res=connection.execute(sql)
hostnames = [ row['hostname'] for row in res ]

with open(args.outfile,'w') as f:
    f_csv = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    f_csv.writerow(['url','is_article? (y/n)','publication_date (yyyy-mm-dd hh:mm:ss)'])
    for i,hostname in enumerate(hostnames):
        if hostname == 'sinonk.com':
            continue
        if i>args.max_hostnames:
            break
        print(f'i={i} hostname={hostname}')

        conds = [
            #(10, '')
            (1, 'and pub_time > now()'),
            (1, 'and pub_time < \'1960-01-01\''),
            (4, 'and pub_time is null '),
            (4, 'and pub_time is not null '),
            #(2, 'and pub_time is null '),
            #(2, 'and pub_time is not null and length(text) < 100'),
            #(4, 'and pub_time is not null and length(text) > 1000'),
            ]
        for limit,cond in conds:
            #print(f'  {cond}')
            sql=sqlalchemy.sql.text(f'''
                select distinct on (url) pub_time,url
                FROM (
                    SELECT --distinct on (length(path))
                        pub_time,
                        scheme || '://' || urls.hostname || path as url
                    FROM articles
                    INNER JOIN urls on urls.id_urls = articles.id_urls
                    WHERE 
                        articles.hostname=:hostname 
                        and random() < {args.threshold}
                        and length(path)>1
                        {cond}
                    limit {limit}
                    )s
                    limit {limit};
                ''')
            res=connection.execute(sql,{'hostname':hostname})
            try:
                for row in res:
                    f_csv.writerow([row['url'],'',''])
                    f.flush()
                    print(f'pub_time={row["pub_time"]}, url={row["url"]}')
            except:
                pass

