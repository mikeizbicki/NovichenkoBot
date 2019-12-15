import datetime
from urllib.parse import urlparse,urlunparse
import sqlalchemy
from sqlalchemy.sql import text
import copy
import re

def reverse_hostname(hostname):
    return hostname[::-1]+'.'

def parse_url(url):
    # normalizing the url converts all domain characters
    # into lower case and ensures non-alphanumeric characters
    # are properly formatted
    from url_normalize import url_normalize
    try:
        url_parsed=urlparse(url_normalize(url))
    except:
        url_parsed=urlparse(url)

    # remove trailing slash from url if present
    #path=url_parsed.path
    #if len(path)>0 and path[-1]=='/':
        #path=path[:-1]

    # this check is necessary for when url=''
    hostname=url_parsed.hostname
    if hostname is None:
        hostname=''

    # don't store port numbers if its the default port
    port=url_parsed.port
    if port is None:
        port=-1

    return {
        'scheme':url_parsed.scheme,
        'hostname':hostname,
        'port':port,
        'path':url_parsed.path,
        'params':url_parsed.params,
        'query':url_parsed.query,
        'fragment':url_parsed.fragment,
        'other':'',
        }

def urlinfo2url(urlinfo):
    if urlinfo['port']==-1:
        netloc=urlinfo['hostname']
    else:
        netloc=urlinfo['hostname']+':'+str(urlinfo['port'])
    url=urlunparse([
        urlinfo['scheme'],
        netloc,    
        urlinfo['path'],
        urlinfo['params'],
        urlinfo['query'],
        urlinfo['fragment'],
        ])
    return url


def get_url_info(connection,url,depth=0,flexible_url=False):
    '''
    This function returns a dictionary containing the information of the urls table in the database.
    If there is no entry for the url in the table, then an entry is created.
    If the url does not satisfy the constraints of the urls table,
    then this function returns None.
    This typically happens when a url is used to uuencode an image.
    The constraint exists to ensure that the database does not get filled up with these useless urls.
    '''
    url_parsed=parse_url(url)

    # manually check to ensure that size constraints are not violated
    # FIXME: this should really be done in the database 
    # since that's where the constrain information is stored,
    # but I can't figure out how to do it.
    # I've previously tried catching the sqlalchemy.exc.DataError exception,
    # which makes the python part of this function work fine,
    # but the failed insert still causes postgres to terminate the transaction,
    # resulting in InternalError exceptions on future inputs.
    # Somehow, we would need to prevent the failed insert from aborting the transaction.
    if flexible_url:
        if len(url_parsed['query'   ])>1024: url_parsed['query'   ]=''
        if len(url_parsed['fragment'])>256 : url_parsed['fragment']=''
        if len(url_parsed['path'    ])>256 : url_parsed['path'    ]=''
            
    if ( len(url_parsed['scheme'])>8 or
         len(url_parsed['hostname'])>253 or
         len(url_parsed['path'])>1024 or 
         len(url_parsed['params'])>256 or
         len(url_parsed['query'])>1024 or 
         len(url_parsed['fragment'])>256 or
         len(url_parsed['other'])>2048 
         ):
        return None

    # insert into urls table if it doesn't exist 
    sql=sqlalchemy.sql.text('''
    insert into urls 
        (scheme,hostname,port,path,params,query,fragment,other,depth)
        values
        (:scheme,:hostname,:port,:path,:params,:query,:fragment,:other,:depth)
    on conflict do nothing
    returning id_urls
    ;
    ''')
    res=connection.execute(sql,depth=depth,**url_parsed).first()

    # url was already in the database, so we need to do a separate search
    # FIXME: is there a way to include this select statement in the insert above?
    # this might improve performance non-trivially since these queries occur very often
    if res is None:
        sql=sqlalchemy.sql.text('''
        select id_urls 
        from urls
        where
            scheme=:scheme and
            hostname=:hostname and
            port=:port and
            path=:path and
            params=:params and
            query=:query and
            fragment=:fragment
        ''')
        res=connection.execute(sql,depth=depth,**url_parsed).first()

    # build the url_info and return
    id_urls=res[0]
    url_info=url_parsed
    url_info['id_urls']=id_urls
    url_info['depth']=depth
    return url_info


def insert_request(connection,url,priority=0,allow_dupes=False,depth=0,flexible_url=False):
    '''
    Inserts a url into the frontier with the specified priority.
    Returns the url_info object of the input url.
    '''

    url_info=get_url_info(connection,url,depth=depth,flexible_url=flexible_url)
    if url_info is None:
        return

    # check if the url already exists in the frontier
    if not allow_dupes:
        sql=sqlalchemy.sql.text('''
        select priority from frontier where id_urls=:id_urls limit 1;
        ''')
        res=connection.execute(sql,{
            'id_urls':url_info['id_urls']
            }).first()

        # if select statement was successful,
        # then update the priority and return
        if res is not None:
            if priority>0:
                sql=sqlalchemy.sql.text('''
                update frontier set priority=priority+:priority where id_urls=:id_urls;
                ''')
                res=connection.execute(sql,{
                    'priority':priority,
                    'id_urls':url_info['id_urls']
                    })
            return url_info

    # we reach this line if either no duplicates were found or allow_dupes is True,
    # so we insert into the table;
    # but first, we adjust the priority based on properties of the url

    # if url_info has non-empty query/fragment/params components,
    # then it is likely to be a duplicate and the priority should be lowered
    if url_info['query'] != '' or url_info['fragment'] != '' or url_info['params'] != '':
        priority-=1000000

    # for each slash in the url, 
    # we add an exponentially increasing penalty to the priority;
    # this helps ensure that we crawl "simpler" urls before "complex" ones,
    # and in particular helps work around broken websites that have unlimited nesting of subfolders
    # (sinonk.com was the motivating example domain)
    priority-=4**url_info['path'].count('/')

    # we want to emphasize a BFS over DFS, 
    # so we penalize based on the depth
    priority-=url_info['depth']**2

    # if there is a year in the path, 
    # then this is likely to be an article,
    # so we up the priority
    if re.match(r'.*([1-3][0-9]{3})',url_info['path']):
        priority+=100

    # if the priority is so low that it would result in underflow,
    # then set the priority to minimum value
    min_priority = -10**38
    if priority < min_priority:
        priority = min_priority

    # insert into table
    sql=sqlalchemy.sql.text('''
    insert into frontier
        (id_urls,timestamp_received,priority,hostname_reversed)
        values
        (:id_urls,:timestamp_received,:priority,:hostname_reversed)
    ''')
    connection.execute(sql,{
        'id_urls':url_info['id_urls'],
        'timestamp_received':datetime.datetime.now(),
        'priority':priority,
        'hostname_reversed':reverse_hostname(url_info['hostname']),
        })
    return url_info


def get_id_articles(connection,urls):
    '''
    Returns the `id_articles` value that corresponds to the input list of urls.
    If no such article exists, then launch a scrapy process to download the article.
    If the download fails, then return `None`.

    NOTE: 
    This function will download all seed urls in the frontier.
    This should probably be fixed so that it only downloads the requested
    article because there can be an unbounded number of seeds in the frontier,
    causing this function to hang indefinitely.

    FIXME: 
    Think more carefully about how to handle non-canonical urls
    and urls that have been downloaded multiple times

    FIXME:
    There is a lot of redundant code and db accesses here that could be streamlined.

    FIXME:
    If a url is already in the frontier, 
    we should probably just increase its priority rather than adding a new item to the frontier.
    '''

    # loop over each url;
    # if the url is not found in the database, 
    # flag that we need to run scrapy to download the urls
    run_scrapy=False
    for url in urls:

        # search the database for the article
        url_info=get_url_info(connection,url,depth=0)
        sql=sqlalchemy.sql.text('''
            select * from id_urls_2_id_articles(:id_urls)
        ''')
        res=connection.execute(sql,{
            'id_urls':url_info['id_urls']
            })
        res_list=res.first()
        if res_list is not None and res_list[0] is not None:
            id_articles=res_list[0]

        # article not found in the db, so we must download it;
        # we add it into the frontier with infinite priority
        else:
            run_scrapy=True
            sql=sqlalchemy.sql.text('''
            insert into frontier
                (id_urls,timestamp_received,priority,hostname_reversed)
                values
                (:id_urls,:timestamp_received,:priority,:hostname_reversed)
            ''')
            connection.execute(sql,{
                'id_urls':url_info['id_urls'],
                'timestamp_received':datetime.datetime.now(),
                'priority':float('inf'),
                'hostname_reversed':reverse_hostname(url_info['hostname']),
                })

    # run scrapy to download articles not in the database
    if run_scrapy:
        import scrapy
        from scrapy.crawler import CrawlerProcess
        from scrapy.utils.project import get_project_settings
        from NovichenkoBot.spiders.general_spider import GeneralSpider
        settings = get_project_settings()
        settings['INFINITY_CRAWLER'] = True
        process = CrawlerProcess(settings)
        process.crawl(GeneralSpider)
        process.start()

    # check the database again and return the result
    id_articles_list=[]
    for url in urls:
        url_info=get_url_info(connection,url,depth=0)
        sql=sqlalchemy.sql.text('''
            select * from id_urls_2_id_articles(:id_urls)
        ''')
        res=connection.execute(sql,{
            'id_urls':url_info['id_urls']
            })
        res_list=res.first()
        if res_list is not None and res_list[0] is not None:
            id_articles=res_list[0]
            id_articles_list.append(id_articles)
        else:
            print(f'FAILED: {url_info["id_urls"]} {url}')

    return id_articles_list


