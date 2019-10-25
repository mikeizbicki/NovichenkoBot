'''
This module is based off of the standard scrapy scheduler
(https://github.com/scrapy/scrapy/blob/master/scrapy/core/scheduler.py)
and a modified version that uses sqlite3
(https://github.com/filyph/scrapy-sqlite)
The modified version above doesn't work with the current scrapy,
however, so this is a partial reimplementation that suits my needs.
'''
import csv
import datetime
from urllib.parse import urlparse,urlunparse
from six.moves.urllib.parse import urljoin
from w3lib.url import safe_url_string
import logging
import sqlalchemy
import scrapy
from scrapy.utils.misc import load_object, create_instance
from scrapy.utils.job import job_dir
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

from NovichenkoBot.sqlalchemy_utils import get_url_info, insert_request
from timeit import default_timer as timer
#import os
#import sys
#unbuffered = os.fdopen(sys.stdout.fileno(), 'w', 0)
#sys.stdout = unbuffered

logger = logging.getLogger(__name__)

class Scheduler(object):
    """
    """
    def __init__(self, stats=None, crawler=None, db=None):
        settings=crawler.settings
        self.HOSTNAME_RESTRICTIONS = settings.getlist('HOSTNAME_RESTRICTIONS')
        self.HOSTNAME_RESTRICTIONS_clause = [hostname[::-1]+'.%' for hostname in self.HOSTNAME_RESTRICTIONS]
        self.MEMQUEUE_HOSTNAMES = settings.getint('MEMQUEUE_HOSTNAMES',default=40)
        self.MEMQUEUE_LIMIT = settings.getint('MEMQUEUE_LIMIT',default=1000)
        
        self.stats = stats
        self.crawler = crawler
        if crawler is not None:
            self.engine = crawler.spider.engine
            self.connection = crawler.spider.connection
        elif db is not None:
            self.engine = sqlalchemy.create_engine(db, connect_args={'timeout': 120})
            self.connection = self.engine.connect()
        self.memqueue = {}

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(stats=crawler.stats, crawler=crawler)

    def open(self, spider):
        self.spider = spider
        self.connection = self.engine.connect()
        return None #self.df.open()

    def close(self, reason=None):
        self.connection.close()
        return None #self.df.close(reason)

    def enqueue_request(self, request):
        insert_request(self.connection,request.url,priority=request.priority)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)
        return True

    def next_request(self):
        if len(self.memqueue.keys()) < self.MEMQUEUE_HOSTNAMES:
            memqueue_values=list(self.memqueue.keys())
            memqueue_where=''.join([' and hostname != ?' for key in memqueue_values])
            restrictions_values=self.HOSTNAME_RESTRICTIONS_clause
            restrictions_where=' or '.join([' hostname_reversed like ?' for hostname in restrictions_values])
            if len(restrictions_where) > 0:
                restrictions_where=f'and ({restrictions_where})'
            all_values=memqueue_values+restrictions_values

            sql=f'''
            select scheme,hostname,port,path,params,query,fragment,frontier.id_frontier,urls.id_urls,depth
            from urls 
            inner join frontier on urls.id_urls=frontier.id_urls
            where 
                timestamp_processed is null
                {memqueue_where}
                {restrictions_where}
            order by priority desc
            limit {self.MEMQUEUE_LIMIT};
            '''
            rows=list(self.connection.execute(sql,all_values))
            for row in rows:
                hostname=row[1]
                self.memqueue[hostname]=self.memqueue.get(hostname,[])+[row]
                self.next_hostname=hostname

        hostnames=sorted(list(self.memqueue.keys()))
        if hostnames==[]:
            return None

        else:
            current_hostname=self.next_hostname
            next_index=(hostnames.index(self.next_hostname)+1)%len(hostnames)
            self.next_hostname=hostnames[next_index]

            row=self.memqueue[current_hostname].pop()

            if self.memqueue[current_hostname]==[]:
                del self.memqueue[current_hostname]

            # extract the values
            scheme=row[0]
            hostname=row[1]
            port=row[2]
            path=row[3]
            params=row[4]
            query=row[5]
            fragment=row[6]
            id_frontier=row[7]
            id_urls=row[8]
            depth=row[9]
            if port=='':
                netloc=hostname
            else:
                netloc=hostname+':'+port
            url=urlunparse([scheme,netloc,path,params,query,fragment])

            # define callback functions for generating a request
            # these functions are defined locally so that they have access to 
            # the information about the request
            def parse_httpbin(response):

                # handle redirects
                id_urls_redirected = None
                if 300 <= response.status < 400 and 'Location' in response.headers:
                    location = safe_url_string(response.headers['location'])
                    redirected_url = urljoin(request.url, location)
                    url_info_redirected=insert_request(
                            self.connection,
                            redirected_url,
                            priority=response.request.priority,
                            depth=response.request.depth,
                            )
                    id_urls_redirected=url_info_redirected['id_urls']

                # create a new row in responses table
                sql='''
                insert into responses
                    (id_frontier,timestamp_received,twisted_status,http_status,dataloss,bytes,id_urls_redirected)
                    values
                    (?,?,?,?,?,?,?);
                '''
                res=self.connection.execute(sql,(
                    id_frontier,
                    datetime.datetime.now(),
                    'Success',
                    response.status,
                    'dataloss' in response.flags,
                    len(response.body),
                    id_urls_redirected
                    ))
                response.id_responses=res.lastrowid

                # run the spider on the response only if the response is not a redirect
                if id_urls_redirected is None:
                    parse_generator=self.crawler.spider.parse(response)
                else:
                    parse_generator=None

                # update the responses table to indicate it is fully processed
                sql='''
                update responses set timestamp_processed=? where id_responses=?
                '''
                res=self.connection.execute(sql,(
                    datetime.datetime.now(),
                    response.id_responses
                    ))

                # the spider may return a generator that yields urls to crawl, 
                # and we must return that generator here to crawl those urls
                return parse_generator

            def errback_httpbin(failure):
                twisted_status=failure.value.__class__.__name__
                twisted_status_long=str(failure.value)
                sql='''
                insert into responses
                    (id_frontier,timestamp_received,twisted_status,twisted_status_long)
                    values
                    (?,?,?,?);
                '''
                self.connection.execute(sql,(
                    id_frontier,
                    datetime.datetime.now(),
                    twisted_status,
                    twisted_status_long,
                    ))

            # generate the request
            request=scrapy.http.Request(url,callback=parse_httpbin,
                                    errback=errback_httpbin)
            #request=scrapy.http.Request(url)
            request.id_urls=id_urls
            request.id_frontier=id_frontier
            request.depth=depth
            
            # update the row to indicate it has been retrieved 
            sql=f"update frontier set timestamp_processed=? where id_frontier=?"
            self.connection.execute(sql,(datetime.datetime.now(),id_frontier))
            self.stats.inc_value('scheduler/dequeued', spider=self.spider)

            return request

    def has_pending_requests(self):
        #print('has_pending_request')
        return True

    #def __len__(self):
        ##FIXME
        #return 0

    def _dqdir(self,jobdir):
        return


# the scheduler can be run directly to perform manual manipulations of the database
if __name__=='__main__':

    # process command line args
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',default='sqlite:///benchmark.db')
    parser.add_argument('--create_db',action='store_true')
    parser.add_argument('--add_seeds',default=None)
    args = parser.parse_args()

    # create database connection
    engine = sqlalchemy.create_engine(args.db)
    connection = engine.connect()

    # create db tables
    if args.create_db: 
        for filename in [
                'sql/create_db.sql',
                'sql/create_triggers_crawl.sql',
                'sql/create_triggers_articles.sql',
                ]:
            with open(filename, 'r') as sql_file:
                import sqlite3
                sqlite3_connection = sqlite3.connect(args.db[10:], timeout=120)
                sqlite3_cursor = sqlite3_connection.cursor()
                sql_script = sql_file.read()
                sqlite3_cursor.executescript(sql_script)
                sqlite3_cursor.close()
                sqlite3_connection.close()

    # add seeds
    if args.add_seeds is not None:
        with open(args.add_seeds) as f:
            reader = csv.DictReader(f)
            for row in reader:
                domain=row['DOMAIN']
                for url in [
                        'http://'+domain,
                        'http://www.'+domain,
                        'https://'+domain,
                        'https://www.'+domain
                        ]:
                    insert_request(connection,url,allow_dupes=True,priority=float('Inf'))
                url_info=get_url_info(connection,'http://'+domain)
                sql='''
                insert into seed_hostnames
                    (hostname,lang,country)
                    values
                    (?,?,?);
                '''
                connection.execute(sql,(url_info['hostname'],row['LANGUAGE'],row['COUNTRY']))

    # close the connection
    connection.close()
