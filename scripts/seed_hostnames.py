# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--seeds_csv',required=True)
args = parser.parse_args()

# imports
import sqlalchemy
import csv

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_url_info,insert_request
from NovichenkoBot.spiders.general_spider import GeneralSpider

# create database connection
engine = sqlalchemy.create_engine(args.db)
connection = engine.connect()

# add seeds
with open(args.seeds_csv) as f:
    
    reader = csv.DictReader(f)
    for row in reader:

        # extract hostname from row
        url_info = get_url_info(connection,row['URL'])
        hostname = url_info['hostname']
        if hostname[:4]=='www.':
            hostname=hostname[4:]
        print(f'adding {hostname}')

        # add seed URLS into the frontier
        for url in [
                'http://'+hostname+'/'+url_info['path'],
                'http://www.'+hostname+'/'+url_info['path'],
                'https://'+hostname+'/'+url_info['path'],
                'https://www.'+hostname+'/'+url_info['path'], 
                ]:
            insert_request(connection,url,allow_dupes=True,priority=float('Inf'))

        # populate the crawlable_hostnames table
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
            'lang':row['LANGUAGE'],
            'country':row['COUNTRY'],
            'priority':row['PRIORITY'],
            })

# close the connection
connection.close()

