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
    #'http://www.granma.cu/cuba/2019-11-05/suspenden-la-decima-liga-nacional-femenina-de-balonmano-05-11-2019-11-11-00',
    #'https://www.prensa-latina.cu/index.php?o=rn&id=318493&SEO=congreso-de-argentina-retoma-proyecto-de-ley-de-alquileres',
    #'https://ezln.eluniversal.com.mx/caracoles-escuela-de-las-nuevas-semillas/',

    'https://www.nknews.org/2019/10/why-north-korean-and-russian-state-media-are-joining-forces-to-fight-fake-news/',
    'https://www.armscontrolwonk.com/archive/1208302/norm-building-and-tear-downs',
    'http://russianforces.org/blog/2007/07/russia_calls_for_cooperation.shtml',

    'https://thediplomat.com/2019/06/north-korea-the-missing-link-in-northeast-asias-air-pollution-fight/',
    'https://foreignpolicy.com/2013/05/23/pulp-liberation-army/',
    'https://thehill.com/policy/defense/overnights/471731-overnight-defense-presented-by-boeing-house-chairmen-demand-answers',
    'https://www.csis.org/analysis/ending-cycle-crisis-and-complacency-us-global-health-security',
    'https://www.janes.com/article/91085/north-korea-releases-images-of-10-september-weapon-test',
    'https://carnegieendowment.org/2019/02/19/benchmarking-second-trump-kim-summit-pub-78407',

    'https://www.washingtonpost.com/news/worldviews/wp/2017/10/02/north-korea-appears-to-have-a-new-internet-connection-thanks-to-the-help-of-a-state-owned-russian-firm/',
    'https://www.nytimes.com/2005/09/19/world/asia/north-korea-says-it-will-abandon-nuclear-efforts.html',
    'https://www.usatoday.com/story/news/world/2019/05/31/north-korea-executes-senior-officials-over-failed-trump-summit-report/1296383001/',
    #'https://www.politico.eu/article/how-us-north-korea-could-stumble-into-world-war-iii/',
    'https://www.politico.com/latest-news-updates/trump-kim-jong-un-meeting-us-north-korea-summit-2018',
    'https://www.foxnews.com/politics/trump-kim-jong-uns-vietnam-summit-joins-long-list-of-key-moments-between-world-leaders-a-timeline',
    'https://www.cnn.com/2019/11/18/asia/north-korea-us-meeting-intl/index.html',
    'https://www.cnbc.com/2018/09/06/north-korean-hackers-will-be-charged-for-sony-pictures-wannacry-ransomware-attacks.html',
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
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        pass

def print_test_quality():
    urltests=get_urltests()
    numtests=len(urltests)
    for testkey in testkeys:
        print(f'hostnames with invalid {testkey}')
        hostnames_to_fix=[]
        for urltest in urltests:
            if urltest['tests'][testkey]=='' or urltest['tests'][testkey]==[] or urltest['tests'][testkey] is None:
                print('  ',urlparse(urltest['url']).hostname)

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
    update_urltests()
    print_test_quality()

