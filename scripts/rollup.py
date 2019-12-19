# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--max_rollup_size',type=int,default=1000000)
parser.add_argument('--name',required=True)
args = parser.parse_args()

# imports
import sqlalchemy
import os
import datetime

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_url_info,insert_request

# create database connection
engine = sqlalchemy.create_engine(args.db)
connection = engine.connect()

# call the postgresql function do_rollup() until the entire table is rolled
rows_rolled=0
while True:
    print(datetime.datetime.now(),'rows_rolled=',rows_rolled)

    sql = sqlalchemy.sql.text('''
        SELECT * FROM do_rollup(:name,:max_rollup_size);
    ''')
    res = connection.execute(sql,{
        'name':args.name,
        'max_rollup_size':args.max_rollup_size
        })
    row = res.first()
    new_rows = row[1]-row[0]
    rows_rolled += new_rows

    if new_rows < args.max_rollup_size:
        break

# close the connection
connection.close()

