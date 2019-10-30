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
from sqlalchemy.sql import text
import scrapy
from scrapy.utils.misc import load_object, create_instance
from scrapy.utils.job import job_dir
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

from NovichenkoBot.sqlalchemy_utils import get_url_info, urlinfo2url, insert_request, reverse_hostname
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
        self.HOSTNAME_RESTRICTIONS_clause = [reverse_hostname(hostname)+'%' for hostname in self.HOSTNAME_RESTRICTIONS]
        self.MEMQUEUE_HOSTNAMES = settings.getint('MEMQUEUE_HOSTNAMES',default=40)
        self.MEMQUEUE_LIMIT = settings.getint('MEMQUEUE_LIMIT',default=1000)
        self.MEMQUEUE_MIN = settings.getint('MEMQUEUE_MIN',default=100)
        
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

    def close(self, reason=None):
        self.connection.close()

    def enqueue_request(self, request):
        insert_request(self.connection,request.url,priority=request.priority)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)
        return True

    def next_request(self):
        # whenever the number of keys in the memqueue is small enough,
        # we must select new rows from the frontier to fill the memqueue;
        memqueue_values=sum(map(len,self.memqueue.values()))
        #if len(self.memqueue.keys()) < self.MEMQUEUE_HOSTNAMES:
        if memqueue_values < self.MEMQUEUE_MIN:
            logger.info(f'expanding memqueue; keys = {len(self.memqueue.keys())} ; values = {memqueue_values}')
            
            # the query to fill the memqueue is rather complicated
            # and divided into several parts;
            # all the parts will store parameters in this values_dict
            values_dict={}

            # generate a where clause for ensuring that the returned hostnames
            # do not match any hostnames already in the dictionary;
            # this ensures a broad crawl and that we don't send too much traffic
            # to a single host
            memqueue_where=''
            memqueue_index=0
            for hostname in self.memqueue.keys():
                memqueue_index+=1
                memqueue_where+=f' and hostname_reversed != :hostname_reversed{memqueue_index} '
                values_dict[f'hostname_reversed{memqueue_index}']=reverse_hostname(hostname)

            # generate a where clause that ensures we are only crawling allowed
            # domains and subdomains based on the HOSTNAME_RESTRICTIONS parameter
            restrictions=[]
            restrictions_index=0
            for hostname_reversed in self.HOSTNAME_RESTRICTIONS_clause:
                restrictions_index+=1
                restrictions.append(f' hostname_reversed like :hostname_reversed{restrictions_index}')
                values_dict[f'hostname_reversed{restrictions_index}']=hostname_reversed
            restrictions_where=' or '.join(restrictions)
            if len(restrictions_where) > 0:
                restrictions_where=f'and ({restrictions_where})'

            # substitute the above where constraints into the sql,
            # execute the result, and add it to memqueue
            sql=text(f'''
            select scheme,hostname,port,path,params,query,fragment,frontier.id_frontier,urls.id_urls,depth
            from urls 
            inner join frontier on urls.id_urls=frontier.id_urls
            where 
                timestamp_processed is null
                {memqueue_where}
                {restrictions_where}
            order by priority desc
            limit {self.MEMQUEUE_LIMIT};
            ''')
            #res=connection.execute(sql,url_parsed)
            #return [{column: value for column, value in row.items()} for row in res][0]
            res=self.connection.execute(sql,values_dict)
            for row in [dict(row.items()) for row in res]:
                hostname=row['hostname']
                self.memqueue[hostname]=self.memqueue.get(hostname,[])+[row]
                self.next_hostname=hostname

            logger.info(f'expanded memqueue; keys = {len(self.memqueue.keys())} ; values = {memqueue_values}')

        # if the memqueue is empty, then there are no pages in the frontier
        # and we should return
        hostnames=sorted(list(self.memqueue.keys()))
        if hostnames==[]:
            return None

        # otherwise generate the next request
        else:
            # these lines ensure that everytime next_request is called,
            # the request comes from a different entry in the memqueue
            current_hostname=self.next_hostname
            next_index=(hostnames.index(self.next_hostname)+1)%len(hostnames)
            self.next_hostname=hostnames[next_index]

            # get the next row from the frontier
            frontier_row=self.memqueue[current_hostname].pop()
            url=urlinfo2url(frontier_row)
            
            # if the pop leaves an entry of the memqueue empty,
            # delete the key so that a new entry can be populated
            if self.memqueue[current_hostname]==[]:
                del self.memqueue[current_hostname]

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
                sql=text('''
                insert into responses
                    (id_frontier,timestamp_received,twisted_status,http_status,dataloss,bytes,id_urls_redirected)
                    values
                    (:id_frontier,:timestamp_received,:twisted_status,:http_status,:dataloss,:bytes,:id_urls_redirected)
                    returning id_responses
                ''')
                res=self.connection.execute(sql,{
                    'id_frontier':frontier_row['id_frontier'],
                    'timestamp_received':datetime.datetime.now(),
                    'twisted_status':'Success',
                    'http_status':response.status,
                    'dataloss':'dataloss' in response.flags,
                    'bytes':len(response.body),
                    'id_urls_redirected':id_urls_redirected
                    })
                #response.id_responses=res.lastrowid
                response.id_responses=res.first()[0]

                # run the spider on the response only if the response is not a redirect
                if id_urls_redirected is None:
                    parse_generator=self.crawler.spider.parse(response)
                else:
                    parse_generator=None

                # update the responses table to indicate it is fully processed
                sql=text('''
                update responses set timestamp_processed=:timestamp_processed where id_responses=:id_responses
                ''')
                res=self.connection.execute(sql,{
                    'timestamp_processed':datetime.datetime.now(),
                    'id_responses':response.id_responses
                    })

                # the spider may return a generator that yields urls to crawl, 
                # and we must return that generator here to crawl those urls
                return parse_generator

            def errback_httpbin(failure):
                twisted_status=failure.value.__class__.__name__
                twisted_status_long=str(failure.value)
                sql=text('''
                insert into responses
                    (id_frontier,timestamp_received,twisted_status,twisted_status_long)
                    values
                    (:id_frontier,:timestamp_received,:twisted_status,:twisted_status_long);
                ''')
                self.connection.execute(sql,{
                    'id_frontier':frontier_row['id_frontier'],
                    'timestamp_received':datetime.datetime.now(),
                    'twisted_status':twisted_status,
                    'twisted_status_long':twisted_status_long,
                    })

            # generate the request
            request=scrapy.http.Request(url,callback=parse_httpbin,
                                    errback=errback_httpbin)
            #request=scrapy.http.Request(url)
            request.id_urls=frontier_row['id_urls']
            request.id_frontier=frontier_row['id_frontier']
            request.depth=frontier_row['depth']
            
            # update the row to indicate it has been retrieved 
            sql=text(f'''
            update frontier 
                set timestamp_processed=:timestamp_processed 
                where id_frontier=:id_frontier
            ''')
            self.connection.execute(sql,{
                'timestamp_processed':datetime.datetime.now(),
                'id_frontier':frontier_row['id_frontier']
                })
            self.stats.inc_value('scheduler/dequeued', spider=self.spider)

            return request

    def has_pending_requests(self):
        # FIXME
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

                # add hostnames into the hostnames table if they don't exist
                # if they do exist, postgres raises an error, 
                # which we catch and discard
                try:
                    sql=text('''
                    insert into seed_hostnames
                        (hostname,lang,country)
                        values
                        (:hostname,:lang,:country);
                    ''')
                    connection.execute(sql,{
                        'hostname':url_info['hostname'],
                        'lang':row['LANGUAGE'],
                        'country':row['COUNTRY']
                        })
                except sqlalchemy.exc.IntegrityError:
                    pass

    # close the connection
    connection.close()
