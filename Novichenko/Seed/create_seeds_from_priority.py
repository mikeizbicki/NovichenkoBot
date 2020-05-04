# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--allow_dupes',action='store_true')
parser.add_argument('--priority',required=True)
args = parser.parse_args()

# imports
import sqlalchemy
import os
import datetime

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from Novichenko.Bot.sqlalchemy_utils import get_url_info,insert_request

# create database connection
engine = sqlalchemy.create_engine(args.db)
connection = engine.connect()

# insert seeds into db
sql=sqlalchemy.sql.text('''
SELECT hostname 
FROM crawlable_hostnames
where priority=:priority
''')
res = connection.execute(sql,{
    'priority':args.priority,
    })

with connection.begin() as trans:
    for row in res:
        url = 'https://'+row['hostname']
        print('inserting',url)
        url_info = insert_request(
                connection,
                url,
                allow_dupes=args.allow_dupes,
                priority=float('Inf'),
                flexible_url=True
                )

# close the connection
connection.close()



