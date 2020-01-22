#!/bin/python3
'''
FIXME: 
Add support for UK/MX parliaments
https://members.parliament.uk/members/Lords
https://www.senado.gob.mx/64/senadores

Some members in these countries don't have webpages, 
but instead have social media accounts
'''

# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--source',required=True,choices=['us_senate','us_house'])
parser.add_argument('--dryrun',action='store_true')
parser.add_argument('--verbose',action='store_true')
args = parser.parse_args()

# imports
import sqlalchemy
import os
import datetime
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_id_hostnames

# create database connection
engine = sqlalchemy.create_engine(args.db)
connection = engine.connect()

# dict taken from https://gist.github.com/rogerallen/1583593
us_state_abbrev = {
    'American Samoa': 'AS',
    'Guam': 'GU',
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Palau': 'PW',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
}

with connection.begin() as trans:
    if args.source=='us_senate':
        url = 'https://www.senate.gov/general/contact_information/senators_cfm.xml'
        user_agent = {'User-agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=user_agent)
        bs = BeautifulSoup(r.text, features='xml')

    if args.source=='us_house':
        url = 'https://www.house.gov/representatives'
        r = requests.get(url)
        bs = BeautifulSoup(r.text, features='lxml')

        for tr in bs.find_all('div',class_='view-content')[1].find_all('tr'):
            try:
                name = tr.find_all('td')[0].text
            except IndexError:
                continue 
            name_given = name.split(',')[1]
            name_family = name.split(',')[0]
            url = tr.find_all('td')[0].find('a')['href']
            hostname = urlparse(url).netloc
            if hostname == 'clerk.house.gov':
                hostname = None
            id_hostnames = get_id_hostnames(connection,hostname)
            location = tr.find_all('td')[1].text.strip()
            state = ' '.join(location.split(' ')[:-1]).strip()
            district = location.split(' ')[-1].strip()
            if district == 'Large' or district == 'Commissioner':
                state = ' '.join(location.split(' ')[:-2]).strip()
                district = location.split(' ')[-2].strip()
            state_code = us_state_abbrev[state]
            party = tr.find_all('td')[2].text.strip()
            committees = [ li.text for li in tr.find_all('td')[5].find_all('li') ]
            if args.verbose:
                print(f'{party} {name} {hostname} "{state_code}" {district} {committees}')
            if not args.dryrun:
                sql=sqlalchemy.sql.text('''
                    INSERT INTO politicians (role,id_hostnames,name_given,name_family,loc_country,loc_state,party)
                    VALUES ('us-congress-116',:id_hostnames,:name_given,:name_family,:loc_country,:loc_state,:party)
                    ''')
                res=connection.execute(sql,{
                    'id_hostnames':id_hostnames,
                    'name_given':name_given,
                    'name_family':name_family,
                    'loc_country':'us',
                    'loc_state':state_code,
                    'party':party,
                    })
