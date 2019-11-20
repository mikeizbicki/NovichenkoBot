'''
this file tests the get_url_info function to ensure that it can handle very long urls
'''

import pytest
import sqlalchemy

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_url_info

def test_get_url_info():
    # FIXME: the database should be parameterized somehow
    engine = sqlalchemy.create_engine('postgresql:///novichenkobot') 
    connection = engine.connect()
    url='https://www.nytimes.com/'+'absurd_path'*1000
    get_url_info(connection,url)
    return True
