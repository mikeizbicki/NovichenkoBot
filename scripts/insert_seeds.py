# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--allow_dupes',action='store_true')
parser.add_argument('--inputs',required=True)
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

# add seeds
try:
    files = [ os.path.join(args.inputs,file) for file in sorted(os.listdir(args.inputs)) ]
except OSError:
    files = [ args.inputs ]

for file in files:
    print(datetime.datetime.now(),file)
    with open(file) as f:
        for line in f:
            url = line.strip()
            insert_request(
                    connection,
                    url,
                    allow_dupes=args.allow_dupes,
                    priority=float('Inf'),
                    flexible_url=True
                    )

# close the connection
connection.close()


