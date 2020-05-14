#!/usr/bin/python3

# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--inputs',nargs='+',required=True)
parser.add_argument('--print_every',type=int,default=1000)
args = parser.parse_args()

# imports
import sqlalchemy
import os
import datetime
import zipfile
import io
import simplejson as json

# create database connection
engine = sqlalchemy.create_engine(args.db)
connection = engine.connect()

# loop through file
for filename in args.inputs:
    with zipfile.ZipFile(filename, 'r') as archive: 
        #with connection.begin() as trans:
            print(datetime.datetime.now(),filename)
            for subfilename in archive.namelist():
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    for i,line in enumerate(f):
                        if i%args.print_every==0:
                            print(datetime.datetime.now(),filename,subfilename,'i=',i)
                        try:
                            sql=sqlalchemy.sql.text('''
                            INSERT INTO twitter VALUES (:data)
                            ON CONFLICT DO NOTHING;
                            ''')
                            res = connection.execute(sql,{'data':line})
                        except sqlalchemy.exc.DataError:
                            sql=sqlalchemy.sql.text('''
                            INSERT INTO twitter_null VALUES (:data)
                            ON CONFLICT DO NOTHING;
                            ''')
                            res = connection.execute(sql,{'data':line})

