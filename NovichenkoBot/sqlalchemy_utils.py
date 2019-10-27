import datetime
from urllib.parse import urlparse,urlunparse
from sqlalchemy.sql import text
import copy

def parse_url(url):
    # normalizing the url converts all domain characters
    # into lower case and ensures non-alphanumeric characters
    # are properly formatted
    from url_normalize import url_normalize
    url_parsed=urlparse(url_normalize(url))

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


def get_url_info(connection,url,depth=0):
    url_parsed=parse_url(url)

    # insert into urls table if it doesn't exist 
    sql=text('''
    insert into urls 
        (scheme,hostname,port,path,params,query,fragment,other,depth)
        values
        (:scheme,:hostname,:port,:path,:params,:query,:fragment,:other,:depth)
    on conflict do nothing;
    ''')
    res=connection.execute(sql,depth=depth,**url_parsed)

    # query to find the id_urls
    sql=text('''
    select id_urls,scheme,hostname,port,path,params,query,fragment,other,depth from urls where
        scheme=:scheme and
        hostname=:hostname and
        port=:port and
        path=:path and
        params=:params and
        query=:query and
        fragment=:fragment
    ''')
    res=connection.execute(sql,url_parsed)
    return [{column: value for column, value in row.items()} for row in res][0]

def insert_request(connection,url,priority=0,allow_dupes=False,depth=0):
    # if a url has already been indexed that has a similar structure
    # (and therefore this url is likely to be a duplicate), 
    # then lower the priority
    url_parsed=parse_url(url)
    sql=text('''
    select id_urls from urls where 
        hostname=:hostname and 
        path=:path;
    ''')
    values=copy.deepcopy(url_parsed)
    if len(values['path'])>0:
        values['path']=values['path'][:-1]+'%'
    res=connection.execute(sql,values)
    url_info=get_url_info(connection,url,depth=depth)
    if res is not None and url_info['id_urls'] != res.first():
        priority-=10

    # check if the url already exists in the frontier
    if not allow_dupes:
        sql=text('''
        select priority from frontier where id_urls=:id_urls limit 1;
        ''')
        res=connection.execute(sql,{
            'id_urls':url_info['id_urls']
            }).first()
        # if select statement was successful,
        # then update the priority and return
        if res is not None:
            if priority>0:
                sql=text('''
                update frontier set priority=priority+:priority where id_urls=:id_urls;
                ''')
                res=connection.execute(sql,{'priority',priority,'id_urls',url_info['id_urls']})
            return url_info

    # we reach this line if either no duplicates were found or allow_dupes is True,
    # so we insert into the table
    hostname=url_info['hostname']
    hostname_reversed=hostname[::-1]+'.'
    sql=text('''
    insert into frontier
        (id_urls,timestamp_received,priority,hostname_reversed)
        values
        (:id_urls,:timestamp_received,:priority,:hostname_reversed)
    ''')
    connection.execute(sql,{
        'id_urls':url_info['id_urls'],
        'timestamp_received':datetime.datetime.now(),
        'priority':priority,
        'hostname_reversed':hostname_reversed,
        })
    return url_info
