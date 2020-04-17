# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--allow_dupes',action='store_true')
parser.add_argument('--crawlable_hostnames_priority')
parser.add_argument('--inputs',required=True)
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

# add seeds
try:
    files = [ os.path.join(args.inputs,file) for file in sorted(os.listdir(args.inputs)) ]
except OSError:
    files = [ args.inputs ]

for file in files:
    print(datetime.datetime.now(),file)
    with open(file) as f:
        for i,line in enumerate(f):
            if i%10==0:
                print(datetime.datetime.now(),'i=',i)
            url = line.strip()
            url_info = insert_request(
                    connection,
                    url,
                    allow_dupes=args.allow_dupes,
                    priority=float('Inf'),
                    flexible_url=True
                    )
            hostname = url_info['hostname']

            # populate the crawlable_hostnames table
            if args.crawlable_hostnames_priority is not None:

                sql=sqlalchemy.sql.text('''
                SELECT priority
                FROM crawlable_hostnames
                where hostname=:hostname
                ''')
                res = connection.execute(sql,{
                    'hostname':hostname,
                    }).first()

                if res is None or res['priority']!='high':
                    sql=sqlalchemy.sql.text('''
                    INSERT INTO crawlable_hostnames
                        (hostname,lang,country,priority)
                        VALUES
                        (:hostname,:lang,:country,:priority)
                    ON CONFLICT (hostname)
                    DO UPDATE
                        SET lang=:lang,country=:country,priority=:priority
                        ;
                    ''')
                    connection.execute(sql,{
                        'hostname':hostname,
                        'lang':'',
                        'country':'',
                        'priority':args.crawlable_hostnames_priority,
                        })

# close the connection
connection.close()


