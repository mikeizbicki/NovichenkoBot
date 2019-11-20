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

logger = logging.getLogger(__name__)

class Scheduler(object):
    """
    """
    def __init__(self, stats=None, crawler=None, db=None):
        settings=crawler.settings
        self.INFINITY_CRAWLER = settings.getbool('INFINITY_CRAWLER',False)
        self.HOSTNAME_RESTRICTIONS = settings.getlist('HOSTNAME_RESTRICTIONS')
        self.HOSTNAME_RESTRICTIONS_loop = [
                reverse_hostname(hostname)
                for hostname in self.HOSTNAME_RESTRICTIONS+['www.'+hostname for hostname in self.HOSTNAME_RESTRICTIONS]
                ]
        self.MEMQUEUE_LIMIT = settings.getint('MEMQUEUE_LIMIT',default=1000)
        self.MEMQUEUE_MIN_URLS = settings.getint('MEMQUEUE_MIN_URLS',default=100)
        self.MEMQUEUE_MAX_URLS = settings.getint('MEMQUEUE_MAX_URLS',default=self.MEMQUEUE_LIMIT*2)
        self.MEMQUEUE_TIMEDELTA = settings.getint('MEMQUEUE_TIMEDELTA',default=120)
        self.time_of_last_memqueue_fill = datetime.datetime.now()- datetime.timedelta(0,1000*self.MEMQUEUE_TIMEDELTA)
        
        self.stats = stats
        self.crawler = crawler
        if crawler is not None:
            self.connection = crawler.spider.connection
        elif db is not None:
            engine = sqlalchemy.create_engine(db, connect_args={'timeout': 120})
            self.connection = engine.connect()
        self.memqueue = {}

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(stats=crawler.stats, crawler=crawler)

    def open(self, spider):
        self.spider = spider

    def close(self, reason=None):
        self.connection.close()

    def enqueue_request(self, request):
        insert_request(self.connection,request.url,priority=request.priority)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)
        return True

    def next_request(self):

        self._update_memqueue()

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

                    # if we are in INFINITY_CRAWLER mode,
                    # then the redirects need to have high priority
                    # FIXME: there should be a more elegant way to do this
                    # where the priority of redirects is equal to the priority
                    # of the original request (and this would help normal crawls
                    # as well)
                    priority=response.request.priority
                    if self.INFINITY_CRAWLER:
                        priority=float('inf')

                    # insert a new request to the redirected url
                    location = safe_url_string(response.headers['location'])
                    redirected_url = urljoin(request.url, location)
                    url_info_redirected=insert_request(
                            self.connection,
                            redirected_url,
                            priority=priority,
                            depth=response.request.depth,
                            )
                    id_urls_redirected=url_info_redirected['id_urls']

                # create a new row in responses table
                sql=text('''
                insert into responses
                    (id_frontier,hostname,timestamp_received,twisted_status,http_status,dataloss,bytes,id_urls_redirected)
                    values
                    (:id_frontier,:hostname,:timestamp_received,:twisted_status,:http_status,:dataloss,:bytes,:id_urls_redirected)
                    returning id_responses
                ''')
                res=self.connection.execute(sql,{
                    'id_frontier':frontier_row['id_frontier'],
                    'hostname':frontier_row['hostname'],
                    'timestamp_received':datetime.datetime.now(),
                    'twisted_status':'Success',
                    'http_status':response.status,
                    'dataloss':'dataloss' in response.flags,
                    'bytes':len(response.body),
                    'id_urls_redirected':id_urls_redirected
                    })
                response.id_responses=res.first()[0]

                # run the spider on the response only if the response is not a redirect
                parse_generator=None
                parse_error=None
                if id_urls_redirected is None:
                    try:
                        parse_generator=self.crawler.spider.parse(response)
                    except e:
                        parse_error=str(e)[:2048]
                        logger.error('parse error {parse_error} on {request.url}')
                        logger.error(traceback.format_exc())

                # update the responses table to indicate it is fully processed
                sql=text('''
                update responses set timestamp_processed=:timestamp_processed where id_responses=:id_responses
                ''')
                res=self.connection.execute(sql,{
                    'timestamp_processed':datetime.datetime.now(),
                    'id_responses':response.id_responses,
                    #'parse_error':parse_error,
                    })

                # the spider may return a generator that yields urls to crawl, 
                # and we must return that generator here to crawl those urls
                return parse_generator

            def errback_httpbin(failure):
                twisted_status=failure.value.__class__.__name__
                twisted_status_long=str(failure.value)
                sql=text('''
                insert into responses
                    (id_frontier,hostname,timestamp_received,twisted_status,twisted_status_long)
                    values
                    (:id_frontier,:hostname,:timestamp_received,:twisted_status,:twisted_status_long);
                ''')
                self.connection.execute(sql,{
                    'id_frontier':frontier_row['id_frontier'],
                    'hostname':frontier_row['hostname'],
                    'timestamp_received':datetime.datetime.now(),
                    'twisted_status':twisted_status,
                    'twisted_status_long':twisted_status_long,
                    })

            # generate the request
            request=scrapy.http.Request(
                    url,
                    callback=parse_httpbin,
                    errback=errback_httpbin
                    )
            request.id_urls=frontier_row['id_urls']
            request.hostname=frontier_row['hostname']
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

        # when operating in normal mode, the spider should continue running
        # even if there is nothing in the memqueue; therefore we always
        # return True
        if not self.INFINITY_CRAWLER:
            return True

        # when operating as the INFINITY_CRAWLER, then we need to exit
        # after downloading all of the frontier items with infinite priority
        else:
            self._update_memqueue()
            hostnames=sorted(list(self.memqueue.keys()))
            print('hostnames=',hostnames)
            if hostnames==[]:
                return False
            else:
                return True


    def _update_memqueue(self):
        '''
        This function fills the memqueue from the database.
        It is intended as a helper function for the next_request function.
        '''

        # whenever the number of keys in the memqueue is small enough,
        # we must select new rows from the frontier to fill the memqueue;
        memqueue_values=sum(map(len,self.memqueue.values()))
        tdelta=datetime.datetime.now()-self.time_of_last_memqueue_fill
        if ( memqueue_values < self.MEMQUEUE_MAX_URLS and 
             tdelta.seconds > self.MEMQUEUE_TIMEDELTA and 
             memqueue_values < self.MEMQUEUE_MIN_URLS
             ):

            # output some debugging information whenever we fill the memqueue
            self.time_of_last_memqueue_fill=datetime.datetime.now()
            logger.info(f'expanding memqueue; keys = {self.memqueue.keys()} ; values = {memqueue_values}')

            # if INFINITY_CRAWLER is set, then we crawl anything in the frontier
            # with an infinite priority; these are urls that are added as seeds
            if self.INFINITY_CRAWLER:
                sql=text(f'''
                select 
                    scheme,
                    hostname,
                    port,
                    path,
                    params,
                    query,
                    fragment,
                    fmod.id_frontier,
                    urls.id_urls,
                    depth
                from urls 
                inner join (
                    select 
                        id_frontier,
                        id_urls
                    from frontier 
                    where
                        timestamp_processed is null AND
                        priority = '+infinity'
                    limit {self.MEMQUEUE_LIMIT}
                    ) as fmod on urls.id_urls=fmod.id_urls
                    ;
                ''')

                # execute the SQL query and update memqueue with the results
                res=self.connection.execute(sql,values_dict)
                for row in [dict(row.items()) for row in res]:
                    hostname=row['hostname']
                    self.memqueue[hostname]=self.memqueue.get(hostname,[])+[row]
                    self.next_hostname=hostname

            # performing a normal crawl restricted to certain domains
            else:
                for hostname_reversed in self.HOSTNAME_RESTRICTIONS_loop:
                    sql=text(f'''
                    select scheme,hostname,port,path,params,query,fragment,fmod.id_frontier,urls.id_urls,depth
                    from urls 
                    inner join (
                        select id_frontier,id_urls
                        from frontier 
                        where
                            timestamp_processed is null and
                            hostname_reversed=:hostname_reversed
                            order by priority desc
                            limit {self.MEMQUEUE_LIMIT}
                        ) as fmod on urls.id_urls=fmod.id_urls
                        ;
                    ''')

                    # execute the SQL query and update memqueue with the results
                    res=self.connection.execute(sql,{
                        'hostname_reversed':hostname_reversed
                        })
                    for row in [dict(row.items()) for row in res]:
                        hostname=row['hostname']
                        self.memqueue[hostname]=self.memqueue.get(hostname,[])+[row]
                        self.next_hostname=hostname


            memqueue_values=sum(map(len,self.memqueue.values()))
            logger.info(f'expanded memqueue; keys = {self.memqueue.keys()} ; values = {memqueue_values}')


