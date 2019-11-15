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

#testkeys=['text','title','html','publish_date','lang','authors']
testkeys=['publish_date','lang','authors']

urls=[
    'http://www.granma.cu/cuba/2019-11-05/suspenden-la-decima-liga-nacional-femenina-de-balonmano-05-11-2019-11-11-00',
    'https://www.nknews.org/2019/10/why-north-korean-and-russian-state-media-are-joining-forces-to-fight-fake-news/',
    'https://www.prensa-latina.cu/index.php?o=rn&id=318493&SEO=congreso-de-argentina-retoma-proyecto-de-ley-de-alquileres',
    'https://ezln.eluniversal.com.mx/caracoles-escuela-de-las-nuevas-semillas/',
    'https://www.armscontrolwonk.com/archive/1208302/norm-building-and-tear-downs',
    ]

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
        r=requests.get(url)
        html=r.text
        with open(filename,'x') as f:
            f.write(html)
    return html

def update_urltests():
    urltests=[]
    for url in urls:
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
        urltests.append({'url':url,'tests':tests})

    with open(urltests_file,'w') as f:
        f.write(json.dumps(urltests))

def get_urltests():
    try:
        with open(urltests_file) as f:
            return json.load(f)
    except FileNotFoundError:
        pass

def print_test_quality():
    urltests=get_urltests()
    numtests=len(urltests)
    hostnames_to_fix=[]
    print(f'bad test count: (/{numtests})')
    for testkey in testkeys:
        badtests=0
        for urltest in urltests:
            if urltest['tests'][testkey]=='' or urltest['tests'][testkey]==[] or urltest['tests'][testkey] is None:
                badtests+=1
                hostnames_to_fix.append(urlparse(urltest['url']).hostname)
        print(f'{testkey.rjust(20)} : {badtests}')
    print('hostnames_to_fix=',set(hostnames_to_fix))

@pytest.mark.parametrize('urltest',get_urltests())
def test_html2article(urltest):
    html=download_url(urltest['url'])
    article=html2article(urltest['url'],html)

    # for all urls, we check that the articles extracted are non-empty
    assert article.title != ''
    assert article.title is not None
    assert article.text != ''
    assert article.text is not None
    assert article.html != ''
    assert article.html is not None

    # for other aspects of the article, we check to ensure they have a specific value
    if 'publish_date' in urltest:
        assert str(getattr(article,'publish_date')) == urltest['tests']['publish_date']

if __name__=='__main__':
    #update_urltests()
    print_test_quality()

