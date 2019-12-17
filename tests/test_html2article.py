import datetime
import hashlib
import os
import requests
import json
from urllib.parse import urlparse
import pytest

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.spiders.general_spider import html2article

# create global variables that control which test cases get generated
cache_dir='tests/__testcache__'
os.makedirs(cache_dir, exist_ok=True)
urltests_file=os.path.join(cache_dir,'urltests.json')

testkeys=['publish_date','lang','authors']

def download_url(url):
    '''
    either downloads the url or loads the url from cache;
    useful for speeding up tests and ensuring that tests do not depend
    on current network conditions
    '''
    hostname=urlparse(url).hostname
    encoded=hashlib.sha224(url.encode('utf-8')).hexdigest()
    filename=os.path.join(cache_dir,hostname+'_'+encoded)
    try:
        with open(filename) as f:
            html=f.read()
    except FileNotFoundError:
        r=requests.get(url,headers={'User-Agent':'NovichenkoBot'})
        html=r.text
        with open(filename,'x') as f:
            f.write(html)
    return html

def update_urltests(from_scratch=False):

    if from_scratch:
        urltests={}
    else:
        urltests=get_urltests()

    urls=[]
    with open('tests/test_urls') as f:
        for line in f.readlines():
            urls.append(line.strip())
            
    for url in urls:
        if url in urltests.keys():
            tests=urltests[url]
            valid_test=True
            for k in testkeys:
                if tests[k]==None or tests[k]=='' or tests[k]==[]:
                    valid_test=False
            if valid_test:
                continue

        print(f'{datetime.datetime.now()}: {url}')
        html=download_url(url)
        article=html2article(url,html)
        tests = {}
        for k in testkeys:
            attr=getattr(article,k)
            if type(attr) is datetime.datetime:
                tests[k]=str(attr)
            else:
                tests[k]=attr
        urltests[url]=tests

    with open(urltests_file,'w') as f:
        f.write(json.dumps(urltests))

def get_urltests():
    try:
        with open(urltests_file) as f:
            return json.load(f)
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        pass

def print_test_quality():
    urltests=get_urltests()
    numtests=len(urltests)
    for testkey in testkeys:
        print(f'hostnames with invalid {testkey}')
        hostnames_to_fix=[]
        for url,tests in urltests.items():
            if tests[testkey]=='' or tests[testkey]==[] or tests[testkey] is None:
                print('  ',urlparse(url).hostname)

@pytest.mark.parametrize('urltests',get_urltests().items())
def test_html2article(urltests):
    url,tests=urltests
    html=download_url(url)
    article=html2article(url,html)

    # for all urls, we check that the articles extracted are non-empty
    assert article.title != ''
    assert article.title is not None
    assert article.text != ''
    assert article.text is not None
    assert article.html != ''
    assert article.html is not None

    # for other aspects of the article, we check to ensure they have a specific value
    if 'publish_date' in tests:
        assert str(getattr(article,'publish_date')) == tests['publish_date']
    if 'authors' in tests:
        assert type(getattr(article,'authors')) == list
        assert getattr(article,'authors') == tests['authors']
    if 'lang' in tests:
        assert getattr(article,'lang') == tests['lang']

if __name__=='__main__':
    update_urltests()
    print_test_quality()

