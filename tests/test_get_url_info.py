import pytest
import sqlalchemy
import random

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_url_info,urlinfo2url

def test_get_url_info_1(connection):
    '''
    tests that get_url_info can handle very long urls
    '''

    # FIXME: the database should be parameterized somehow
    #engine = sqlalchemy.create_engine('postgresql:///novichenkobot') 
    #connection = engine.connect()
    url='https://www.nytimes.com/'+'absurd_path'*1000
    get_url_info(connection,url)
    return True

def test_get_url_info_3(connection):
    '''
    '''
    url_info={'scheme': 'https', 'hostname': 'media1.faz.net', 'port': -1, 'path': '/ppmedia/aktuell/wirtschaft/2406223479/1.6105689/article_aufmacher_klein/ist-amazon-zu-maechtig.jpg', 'params': '', 'query': '', 'fragment': '', 'other': '', 'depth': 7}
    url=urlinfo2url(url_info)

    # FIXME: the database should be parameterized somehow
    #engine = sqlalchemy.create_engine('postgresql:///novichenkobot') 
    #connection = engine.connect()
    get_url_info(connection,url)
    return True

def test_get_url_info_2(connection):
    '''
    tests that get_url_info returns the same url_id when called twice on
    a new randomly generated url
    '''

    # FIXME: the database should be parameterized somehow
    #engine = sqlalchemy.create_engine('postgresql:///novichenkobot') 
    #connection = engine.connect()
    url = 'https://www.example.com/'+str(random.randrange(100000000000))
    url_info1 = get_url_info(connection,url)
    url_info2 = get_url_info(connection,url)
    return url_info1['id_urls'] == url_info2['id_urls']
