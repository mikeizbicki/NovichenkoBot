import datetime
from urllib.parse import urlparse,urlunparse
from sqlalchemy.sql import text

def parse_url(url):
    # normalizing the url converts all domain characters
    # into lower case and ensures non-alphanumeric characters
    # are properly formatted
    from url_normalize import url_normalize
    url_parsed=urlparse(url_normalize(url))

    # remove trailing slash from url if present
    path=url_parsed.path
    if len(path)>0 and path[-1]=='/':
        path=path[:-1]

    # this check is necessary for when url=''
    hostname=url_parsed.hostname
    if hostname is None:
        hostname=''

    # don't store port numbers if its the default port
    port=url_parsed.port
    if port is None:
        port=''
    if url_parsed.scheme=='http' and port==80:
        port=''
    if url_parsed.scheme=='https' and port==443:
        port=''

    return {
        'scheme':url_parsed.scheme,
        'hostname':hostname,
        'port':port,
        'path':path,
        'params':url_parsed.params,
        'query':url_parsed.query,
        'fragment':url_parsed.fragment,
        }

def get_url_info(connection,url,depth=0):
    url_parsed=parse_url(url)

    # insert into urls table if it doesn't exist 
    sql='''
    insert or ignore into urls 
        (scheme,hostname,port,path,params,query,fragment,other,depth)
        values
        (?,?,?,?,?,?,?,?,?)
    '''
    res=connection.execute(sql,(
        url_parsed['scheme'],
        url_parsed['hostname'],
        url_parsed['port'],
        url_parsed['path'],
        url_parsed['params'],
        url_parsed['query'],
        url_parsed['fragment'],
        '',
        depth,
        ))

    # query to find the id_urls
    sql='''
    select id_urls,scheme,hostname,port,path,params,query,fragment,other,depth from urls where
        scheme=? and
        hostname=? and
        port=? and
        path=? and
        params=? and
        query=? and
        fragment=?
    '''
    res=connection.execute(sql,(
        url_parsed['scheme'],
        url_parsed['hostname'],
        url_parsed['port'],
        url_parsed['path'],
        url_parsed['params'],
        url_parsed['query'],
        url_parsed['fragment'],
        ))
    row=res.first()
    return {
        'id_urls':row[0],
        'scheme':row[1],
        'hostname':row[2],
        'port':row[3],
        'path':row[4],
        'params':row[5],
        'query':row[6],
        'fragment':row[7],
        'other':row[8],
        'depth':row[9],
        }
    id_urls=list(res)[0][0]
    return id_urls,hostname

def insert_request(connection,url,priority=0,allow_dupes=False,depth=0):
    # if a url has already been indexed that has a similar structure
    # (and this is likely to be a duplicate), then lower the priority
    url_parsed=parse_url(url)
    #sql='''
    #select id_urls from urls where 
        #hostname=? and 
        #path=?;
    #'''
    #res=connection.execute(sql,(
        #url_parsed['hostname'],
        #url_parsed['path'],
        #)).first()
    sql=text('''
        select id_urls from urls where 
            hostname=:hostname and 
            path=:path;
        ''')
    res=connection.execute(sql,url_parsed)
    asd
    url_info=get_url_info(connection,url,depth=depth)
    if res is not None and url_info['id_urls'] != res[0]:
        priority-=10

    # add url to urls table and get basic info
    id_urls=url_info['id_urls']
    hostname=url_info['hostname']
    hostname_reversed=hostname[::-1]+'.'

    # check if the url already exists in the frontier
    if not allow_dupes:
        sql='''
        select priority from frontier where id_urls=? limit 1;
        '''
        res=connection.execute(sql,(id_urls,)).first()
        # if select statement was successful,
        # then update the priority and return
        #if res is not None:
            #if priority>0:
                #sql='''
                #update frontier set priority=priority+? where id_urls=?;
                #'''
                #res=connection.execute(sql,(priority,id_urls))
            #return url_info

    # we reach this line if either no duplicates were found
    # or allow_dupes is True
    sql='''
    insert into frontier
        (id_urls,timestamp_received,priority,hostname_reversed)
        values
        (?,?,?,?)
    '''
    connection.execute(sql,(id_urls,datetime.datetime.now(),priority,hostname_reversed))
    return url_info
