#!/bin/python3

# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--source',required=True,choices=['mediabiasfactcheck','allsides'])
parser.add_argument('--dryrun',action='store_true')
parser.add_argument('--verbose',action='store_true')
args = parser.parse_args()

# imports
import sqlalchemy
import os
import datetime

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_id_hostnames

# create database connection
engine = sqlalchemy.create_engine(args.db)
connection = engine.connect()

import requests
from bs4 import BeautifulSoup

with connection.begin() as trans:
    if args.source=='mediabiasfactcheck':
        categories = [
                'left',
                'leftcenter',
                'center',
                'right-center',
                'right',
                'pro-science',
                'conspiracy',
                'fake-news',
                'satire',
                ]
        for category in categories:
            url = f'https://mediabiasfactcheck.com/{category}/'
            print('url=',url)
            r = requests.get(url)
            bs = BeautifulSoup(r.text, features='lxml')
            table = bs.find('table',id='mbfc-table')
            for a in table.find_all('a'):
                #print('a=',a.attr['href'])
                name = a.text.split('(')[0].strip()
                hostname = a.text.split('(')[-1].split(')')[0]
                if hostname[:7]=='http://':
                    hostname = hostname[7:]
                if hostname[:8]=='https://':
                    hostname = hostname[8:]
                if hostname[:4]=='www.':
                    hostname=hostname[4:]
                if hostname.find('.')==-1:
                    hostname = None
                if args.verbose:
                    print(f'"{name}" {hostname} {a["href"]}')
                if not args.dryrun and hostname is not None:
                    id_hostnames = get_id_hostnames(connection,hostname)
                    id_hostnames_www = get_id_hostnames(connection,'www.'+hostname)
                    sql=sqlalchemy.sql.text('''
                        INSERT INTO mediabiasfactcheck (id_hostnames,name,category,label_time)
                        VALUES 
                            (:id_hostnames,:name,:category,now()),
                            (:id_hostnames_www,:name,:category,now())
                        ''')
                    res=connection.execute(sql,{
                        'id_hostnames':id_hostnames,
                        'id_hostnames_www':id_hostnames_www,
                        'name':name,
                        'category':category
                        })

    # FIXME:
    # this needs work to parse the individual pages for each hostname;
    # not sure if it's worth it
    if args.source=='allsides':
        page = 0
        while True:
            page += 1
            url=f'https://www.allsides.com/media-bias/media-bias-ratings?page={page}&field_featured_bias_rating_value=All&field_news_source_type_tid%5B1%5D=1&field_news_source_type_tid%5B2%5D=2&field_news_source_type_tid%5B3%5D=3&field_news_source_type_tid%5B4%5D=4'
            print('url=',url)
            r = requests.get(url)
            bs = BeautifulSoup(r.text, features='lxml')
            trs = bs.find('table').find_all('tr')
            if len(trs) < 2:
                break
            for tr in trs:
                if tr.find('td') is None:
                    continue
                name = tr.find_all('td')[0].text.strip()
                bias = tr.find_all('td')[1].find('a')['href'].split('/')[-1].strip()
                print(f'{name} {bias}')

