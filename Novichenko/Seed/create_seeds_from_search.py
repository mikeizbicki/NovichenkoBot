# #!/usr/bin/python3

# imports
import datetime
from lxml import html
import random
import requests
import time
from urllib.parse import urlparse
import os
import glob
import uuid
import sys

# get the bing api key
key_var_name = 'SEARCH_SUBSCRIPTION_KEY'
if not key_var_name in os.environ:
    raise Exception('Please set/export the environment variable: {}'.format(key_var_name))
subscription_key = os.environ[key_var_name]

# helper function for searches
def search_bing_api(query,count=50,offset=0,freshness=None,mkt=None,lang=None):
    # this is the list of hostnames that we don't want included in the search results;
    banned_hostnames = [
        'reddit.com',
        'youtube.com',
        'twitter.com',
        'wikipedia.org',
        'answers.yahoo.com',
        'imgur.com',
        'amazon.com',
        ]
    query += ' '+' '.join([ '-site:'+hostname for hostname in banned_hostnames ])

    # set market
    markets = {
            'da' : 'da-DK',
            'de' : 'de-DE',
            'en' : 'en-US',
            'es' : 'es-ES',
            'fi' : 'fi-FI',
            'fr' : 'fr-FR',
            'it' : 'it-IT',
            'ja' : 'ja-JP',
            'ko' : 'ko-KR',
            'nl' : 'nl-NL',
            'no' : 'no-NO',
            'pl' : 'pl-PL',
            'pt' : 'pt-BR',
            'ru' : 'ru-RU',
            'sv' : 'sv-SE',
            'tr' : 'tr-TR',
    }
    if lang in markets.keys():
        mkt = markets[lang]
    elif lang is not None and lang[0:2]=='zh':
        urls1 = search_bing_api(query,count,offset,freshness,'zh-CN')
        urls2 = search_bing_api(query,count,offset,freshness,'zh-HK')
        urls3 = search_bing_api(query,count,offset,freshness,'zh-TW')
        return list(set(urls1+urls2+urls3))

    # construct the url
    headers = {
        'Ocp-Apim-Subscription-Key': subscription_key,
    }
    url = 'https://api.cognitive.microsoft.com/bing/v7.0/search' + (
        f'?q={query}'
        f'&count={count}'
        f'&offset={offset}'
        f'&safesearch=Moderate'
        #f'&textDecorations=True'
        #f'&textFormat=HTML'
        f'&responseFilter=Webpages'
        )
    if freshness is not None:
        url += f'&freshness={freshness}'
    if mkt is not None:
        url += f'&mkt={mkt}'

    # initiate request using exponential backoff for failure
    sleep=0
    bad_responses=0
    while True:
        try:
            request = requests.get(url, headers=headers) 
            if request.status_code != 200:
                print('WARNING: request=',request,'bad_responses=',bad_responses)
                bad_responses+=1
                if bad_responses>=3:
                    raise ValueError('too many bad responses')
                time.sleep(30)
            else:
                break
        except requests.exceptions.ConnectionError:
            sleep_time=2**sleep
            print('  sleep_time=',sleep_time)
            time.sleep(sleep_time)
            sleep+=1

    # extract urls from request
    response = request.json()
    try:
        totalEstimatedMatches = response['webPages']['totalEstimatedMatches']
        urls = [ x['url'] for x in response['webPages']['value'] ]
    except:
        urls = []

    # recursively search until count is exhausted;
    # then return accumulated urls
    if count>50 and len(urls)>0:
        return urls+search_bing_api(query,count-50,offset+50,freshness,mkt,lang)
    else:
        return urls


if __name__=='__main__':
    # process command line args
    import argparse
    parser = argparse.ArgumentParser()
    #parser.add_argument('--input_dir',required=True)
    parser.add_argument('--output_file',required=True)
    #parser.add_argument('--lang',required=True)
    parser.add_argument('--search_type',choices=['wide','deep'],required=True)
    #parser.add_argument('--query',required=True)
    parser.add_argument('--query_file')
    parser.add_argument('--prefix_file')
    parser.add_argument('--freshness')
    parser.add_argument('--lang',required=True)
    parser.add_argument('--start_index',default=0,type=int)
    parser.add_argument('--count',default=50,type=int,help='maximum number of urls to return from each search')
    parser.add_argument('--offset',default=0,type=int,help='used for restarting searches')
    args = parser.parse_args()

    # perform the search
    if args.search_type=='wide':
    #def wide_search(query, output_path, start_index=0, search=search_ddg, min_delay=5, max_delay=20):

        # define TLDs
        standardTLDs = ['com', 'net', 'org', 'info', 'edu', 'gov', 'mil', 'int', 'blog']
        ccTLDs = ['ac','ad','ae','af','ag','ai','al','am','an','ao','aq','ar','as','at','au','aw','ax','az','ba','bb','bd','be','bf','bg','bh','bi','bj','bm','bn','bo','br','bs','bt','bv','bw','by','bz','ca','cc','cd','cf','cg','ch','ci','ck','cl','cm','cn','co','cr','cs','cu','cv','cx','cy','cz','dd','de','dj','dk','dm','do','dz','ec','ee','eg','eh','er','es','et','eu','fi','fj','fk','fm','fo','fr','ga','gb','gd','ge','gf','gg','gh','gi','gl','gm','gn','gp','gq','gr','gs','gt','gu','gw','gy','hk','hm','hn','hr','ht','hu','id','ie','il','im','in','io','iq','ir','is','it','je','jm','jo','jp','ke','kg','kh','ki','km','kn','kp','kr','kw','ky','kz','la','lb','lc','li','lk','lr','ls','lt','lu','lv','ly','ma','mc','md','me','mg','mh','mk','ml','mm','mn','mo','mp','mq','mr','ms','mt','mu','mv','mw','mx','my','mz','na','nc','ne','nf','ng','ni','nl','no','np','nr','nu','nz','om','pa','pe','pf','pg','ph','pk','pl','pm','pn','pr','ps','pt','pw','py','qa','re','ro','rs','ru','rw','sa','sb','sc','sd','se','sg','sh','si','sj','sk','sl','sm','sn','so','sr','st','su','sv','sy','sz','tc','td','tf','tg','th','tj','tk','tl','tm','tn','to','tp','tr','tt','tv','tw','tz','ua','ug','uk','us','uy','uz','va','vc','ve','vg','vi','vn','vu','wf','ws','ye','yt','za','zm','zw']
        TLDs = standardTLDs + ccTLDs

        # search each TLD for the query
        mode='x'
        if args.start_index>0:
            mode='a'
        with open(output_file,mode,buffering=1) as f:
            hostnames = set()
            urls = set()
            for i,TLD in enumerate(TLDs):
                if i<start_index:
                    continue
                print(
                    datetime.datetime.now(),'--'
                    'TLD:',TLD,
                    f'({i}/{len(TLDs)})',
                    'hostnames:',len(hostnames),
                    'urls:',len(urls)
                    )
                for url in search_bing_api(args.query+' site:'+TLD,count=args.count,offset=args.offset,lang=args.lang,freshness=args.freshness):
                    f.write(url+'\n')
                    urls.add(url)
                    hostname = urlparse(url).hostname
                    hostnames.add(hostname)

    # deep queries
    if args.search_type=='deep':

        # load the queries
        if args.prefix_file is not None:
            with open(args.prefix_file) as f1:
                lines1 = [ line.strip() for line in f1.readlines() ]
            with open(args.query_file) as f2:
                lines2 = [ line.strip() for line in f2.readlines() ]
            queries = [ line1 + ' ' + line2 for line1 in lines1 for line2 in lines2 ]
        else:
            with open(args.query_file) as f:
                queries = [ line.strip() for line in f.readlines() ]
        queries = list(set(queries))

        # perform the searches
        mode='x'
        if args.start_index>0:
            mode='a'
        try:
            with open(args.output_file,mode,buffering=1) as f:
                hostnames = set()
                urls = set()
                for i,query in enumerate(queries):
                    if i<args.start_index:
                        continue
                    print(
                        datetime.datetime.now(),'--',
                        f'i: {i}/{len(queries)}',
                        f'query=[{query}]',
                        f'hostnames: {len(hostnames)}',
                        f'urls: {len(urls)}',
                        )
                    results = search_bing_api(query,count=args.count,offset=args.offset,lang=args.lang,freshness=args.freshness)
                    for url in results:
                        f.write(url+'\n')
                        urls.add(url)
                        hostname = urlparse(url).hostname
                        hostnames.add(hostname)
        except FileExistsError:
            print('WARNING: file exists... skipping')


sys.exit(0)
################################################################################
#
# the functions below are hacks included only for posterity
#

def search_bing_hack(query, max_results=100, verbose=False):
    results = []
    firsts = [ 1 + 10*i for i in range(max_results//10) ]
    for first in firsts:
        if verbose:
            print('  first=',first,'len(results)=',len(results),'unique=',len(set(results)))
        sleep=0
        while True:
            try:
                res = requests.get(
                    url = 'https://www.bing.com/search?q='+query+'&first='+str(first),
                    headers = {
                        'User-agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-GB; rv:1.8.1.6) Gecko/20070725 Firefox/2.0.0.6'
                        },
                    )
                if res.status_code != 200:
                    raise ValueError(str(res))
                doc = html.fromstring(res.text)
                results_new = [a.get('href') for a in doc.cssselect('li h2 a') if a.get('href')[0] != '/']
                prev_len = len(set(results))
                results += results_new
                new_len = len(set(results))
                with open(f'download.tmp.{first}.html','w') as f:
                    f.write(res.text)
                if new_len-prev_len == 0: #len(results_new) < 10:
                    if 'There are no results for' in res.text:
                        if verbose:
                            print('  no more results')
                        break
                    else:
                        raise ValueError()
                break
            except ValueError:
                sleep_time=2**sleep
                print('  sleep_time=',sleep_time)
                time.sleep(sleep_time)
                sleep+=1
        time.sleep(1)
    return list(set(results))

def search_ddg(query, max_results=10):
    res = requests.post(
        url = 'https://duckduckgo.com/lite/', 
        headers = {
            'User-agent': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
            },
        data = {
            'q': query,
            's': '0',
            }
        )
    if res.status_code != 200:
        raise ValueError(str(res))
    doc = html.fromstring(res.text)
    results = list(set([a.get('href') for a in doc.cssselect('.result-link')]))
    return results

def search(query, search_function=search_bing):
    return search_function(query, max_results=args.max_results, verbose=True)
    # searches usually succeed,
    # however, we using an exponential delay in case of search failure so that we don't
    # overload the search engine
    sleep=0
    while True:
        try:
            results = search_function(query,max_results=args.max_results)
            break
        except:
            sleep_time=30*2**sleep
            print('  sleep_time=',sleep_time)
            time.sleep(sleep_time)
            sleep+=1
    #time.sleep(random.randint(min_delay,max_delay))
    return results
