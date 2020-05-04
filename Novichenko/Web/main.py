from flask import Flask, g, url_for, render_template, request
import sqlalchemy
from sqlalchemy.sql import text
import time
import urllib
import re
from bs4 import BeautifulSoup

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from Novichenko.Bot.sqlalchemy_utils import get_url_info_from_id_urls, urlinfo2url

app = Flask(__name__)


@app.before_request
def before_request():
    g.start = time.time()
    db = 'postgres:///novichenkobot'
    engine = sqlalchemy.create_engine(db, connect_args={
        'connect_timeout': 10,
        'application_name': 'NovichenkoWeb',
        })
    g.connection = engine.connect()


@app.after_request
def after_request(response):
    diff = time.time() - g.start
    if (response.response and not response.direct_passthrough):
        #response.response[0] = response.response[0].replace('__EXECUTION_TIME__', str(diff))
        response.set_data(response.get_data() + f'<p>generation time: {"%0.2f"%diff} seconds </p>'.encode('utf-8'))
    return response


@app.teardown_request
def teardown_request(exception):
    if hasattr(g, 'connection'):
        g.connection.close()


def escape_alphanum(x):
    import re
    return re.sub('[^a-zA-Z0-9_]+', '', x)


def get_alphanum(x,default=None):
    val = request.args.get(x,default)
    if type(val) == str:
        return escape_alphanum(val)
    else:
        return val


def res2html(res,col_formatter=None,transpose=False,click_headers=False):
    rows=[list(res.keys())]+list(res)
    if transpose:
        rows=list(map(list, zip(*rows)))
    html='<table>'
    for i,row in enumerate(rows):
        html+='<tr>'
        if i==0 and not transpose:
            td='th'
            html+=f'<{td}></{td}>'
        else:
            td='td'
            html+=f'<td>{i}</td>'
        for j,col in enumerate(row):
            val = None
            try:
                val = col_formatter(res.keys()[j],col,i==0)
            except:
                if i>0 and col_formatter is not None:
                    val = col_formatter(res.keys()[j],col)
            if val is None:
                val = col
            if type(col) == int or type(col) == float:
                td_class='numeric'
            else:
                td_class='text'
            html+=f'<{td} class={td_class}>{val}</td>'
        html+='</tr>'
    html+='</table>'
    return html


def has_no_empty_params(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)


@app.route('/')
def index():
    links = []
    for rule in app.url_map.iter_rules():
        # Filter out rules we can't navigate to in a browser
        # and rules that require parameters
        if "GET" in rule.methods and has_no_empty_params(rule):
            url = url_for(rule.endpoint, **(rule.defaults or {}))
            links.append((url, rule.endpoint))
    html='<ol>'
    for link in sorted(links):
        html+=f'<li><a href={link[0]}>{link[0]}</a></li>'
    html+='</ol>'

    return render_template(
        'base.html',
        page_name='index',
        page_html=html
        )


@app.route('/static/<path:path>')
def style(path):
    return send_from_directory('flask/static',path)


@app.route('/search')
def search():
    html=''

    query = request.args.get('query')
    query_value = '' if query is None else f'value="{query}"'
    limit = request.args.get('limit', default=100)
    method = request.args.get('method',default='hostname')

    html+=f'''
    <form>
    <input type=text {query_value} name=query style='width:600px'>
    <input type=submit value=search>
    </form>
    '''

    if query is not None:
        if method=='url':
            sql=text(f'''
            SELECT * FROM (
                SELECT distinct on (title) id_articles,articles.hostname,urls.scheme||'://'||articles.hostname||'/'||path as url,pub_time,title
                FROM articles
                INNER JOIN urls ON urls.id_urls = articles.id_urls
                WHERE
                    to_tsquery('english',:query) @@ to_tsvector('english',text)
                    and lang='en'
                    and pub_time is not null
                order by title,pub_time asc
                limit {limit}
                )t
            order by pub_time asc
            ''')
        else:
            sql=text(f'''
            SELECT * FROM (
                SELECT distinct on (title) id_articles,hostname,pub_time,title
                FROM articles
                WHERE
                    to_tsquery('english',:query) @@ to_tsvector('english',text)
                    and lang='en'
                    and pub_time is not null
                order by title,pub_time asc
                limit {limit}
                )t
            order by pub_time asc
            ''')
        res=g.connection.execute(sql,{
            'query':query
            })
        def callback(k,v):
            if k=='url':
                return f'<a href={v}>{v}</a>'
            if k=='hostname':
                return f'<a href=/hostname/{v}>{v}</a>'
            if k=='id_articles':
                return f'<a href="/article/{v}?query={urllib.parse.quote(query)}">{v}</a>'
        html+=res2html(res,callback,click_headers=True)

    return render_template(
            'base.html',
            page_name=f'Search',
            page_html=html
            )

@app.route('/pagerank')
def pagerank():
    html=''
    name=request.args.get('name')

    html+='<h2>pagerank options</h2>'
    sql=text(f'''
    SELECT name
    FROM pagerank
    WHERE id_hostnames=0
    ''')
    res=g.connection.execute(sql)
    def callback(k,v):
        if k=='name':
            link=f'<a href=/pagerank?name={v}>{v}</a>'
            if v==name:
                link=f'<strong>{link}</strong>'
            return link
    html+=res2html(res,callback)

    html+='<h2>pagerank scores</h2>'
    sql=text(f'''
    SELECT hostname,score
    FROM pagerank
    INNER JOIN hostnames ON hostnames.id_hostnames = pagerank.id_hostnames
    WHERE
        name=:name
    ORDER BY score DESC
    LIMIT 1000
    ''')
    res=g.connection.execute(sql,{'name':name})
    def callback(k,v):
        if k=='hostname':
            return f'<a href=/hostname/{v}>{v}</a>'
    html+=res2html(res,callback,click_headers=True)

    return render_template(
            'base.html',
            page_name=f'Pagerank',
            page_html=html
            )


@app.route('/tld')
def tld():
    html=''

    tlds=('.com','.net','.org','.info','.edu','.mil','.gov','.blog','.info','.int')
    #,'.news','.biz','.xyz','.asia','.media','.online','.today','.club','.mobi','.link','.name','.taipei','.global','.press','.travel','.onion','.world')

    html+='<h2>general purpose TLDs</h2>'
    sql=text(f'''
    select substring(hostname from '\.[^\.]+$') as tld, count(1) as num
    from articles_lang_hostnames
    where substring(hostname from '\.[^\.]+$') in {tlds}
    group by tld
    --having count(1)>100
    order by num desc;
    ''')
    res=g.connection.execute(sql)
    def callback(k,v):
        if k=='tld':
            return f'<a href=/hostname_productivity/{v}>{v}</a>'
        return
    html+=res2html(res,callback)

    html+='<h2>country code TLDs</h2>'
    sql=text(f'''
    select substring(hostname from '\.[^\.]+$') as tld, count(1) as num
    from articles_lang_hostnames
    where
        substring(hostname from '\.[^\.]+$') not in {tlds}
        and length(substring(hostname from '\.[^\.]+$'))=3
    group by tld
    order by num desc;
    ''')
    res=g.connection.execute(sql)
    html+=res2html(res,callback)

    html+='<h2>Other TLDs</h2>'
    sql=text(f'''
    select substring(hostname from '\.[^\.]+$') as tld, count(1) as num
    from articles_lang_hostnames
    where
        substring(hostname from '\.[^\.]+$') not in {tlds}
        and length(substring(hostname from '\.[^\.]+$'))!=3
    group by tld
    order by num desc;
    ''')
    res=g.connection.execute(sql)
    html+=res2html(res,callback)

    return render_template(
            'base.html',
            page_name=f'TLD Stats',
            page_html=html
            )


@app.route('/hostname_productivity_lang/<lang>')
def lang_lang(lang):
    html=''

    # get query string parameters
    # FIXME: this is very insecure
    #order_by=request.args.get('order_by')
    #if order_by is None:
        #order_by='fraction_lang'
    #order_by=order_by[:20]

    order_by = request.args.get('order_by', default='fraction_lang')
    limit = request.args.get('limit', default=100)

    order_dir='desc'
    if request.args.get('order_dir') == 'asc':
        order_dir='asc'

    # issue db request
    sql=text(f'''
    /*
    SELECT
        t1.hostname,
        round((num_distinct/sum(num_distinct) over ())::numeric,4) as fraction_lang,
        valid_keywords::int,
        valid_total::int,
        valid_keyword_fraction,
        --round(valid_keyword_fraction::numeric,4) as keyword_fraction,
        --all_keywords::int,
        --all_total::int,
        round(score::numeric,4) as score
    */
    SELECT round((num_distinct/sum(num_distinct) over ())::numeric,4) as fraction_lang,hostname_productivity.*
    FROM (
        SELECT
            hostname,
            sum(#num_distinct) as num_distinct
        FROM articles_lang
        WHERE
            lang=:lang
        GROUP BY hostname
    ) t1
    INNER JOIN hostname_productivity ON t1.hostname = hostname_productivity.hostname
    ORDER BY {order_by} {order_dir}
    LIMIT :limit;
    ''')
    res=g.connection.execute(sql, {
            'lang':lang,
            #'order_by':order_by,
            #'order_dir':order_dir,
            'limit':limit,
            })
    def callback(k,v):
        if k=='hostname':
            return f'<a href=/hostname/{v}>{v}</a>'
    html+=res2html(res,callback,click_headers=True)

    return render_template(
            'base.html',
            page_name=f'Hostname Productivity for {lang}',
            page_html=html
            )


@app.route('/lang')
def lang():
    html=''
    sql=text(f'''
    SELECT
        lang,
        round((num_distinct/sum(num_distinct) over ())::numeric,4) as fraction,
        num_distinct::int
    FROM articles_lang_stats
    ORDER BY num_distinct DESC;
    ''')
    res=g.connection.execute(sql)
    def callback(k,v):
        if k=='lang':
            return f'<a href=/hostname_productivity_lang/{v}>{v}</a>'
    html+=res2html(res,callback)
    return render_template(
            'base.html',
            page_name='Overall Language Statistics',
            page_html=html
            )


@app.route('/responses_summary')
def responses_summary():
    html=''

    sql=text(f'''
    select * from responses_summary order by timestamp desc limit 100;
    ''')
    res=g.connection.execute(sql)
    html+=res2html(res)

    return render_template(
        'base.html',
        page_name='responses summary',
        page_html=html,
        refresh=60
        )


@app.route('/responses_recent_1hr')
def recent_1hr():
    html=''

    sql=text(f'''
    select * from responses_timestamp_hostname_recent_1hr order by num desc;
    ''')
    res=g.connection.execute(sql)
    def callback(k,v):
        if k=='hostname':
            return f'<a href=/hostname/{v}>{v}</a>'
    html += res2html(res,callback)

    return render_template(
            'base.html',
            page_name='prev 24hr stats',
            page_html=html
            )



@app.route('/responses_recent')
def recent():
    html=''

    sql=text(f'''
    select * from responses_timestamp_hostname_recent order by num desc;
    ''')
    res=g.connection.execute(sql)
    def callback(k,v):
        if k=='hostname':
            return f'<a href=/hostname/{v}>{v}</a>'
    html += res2html(res,callback)

    return render_template(
            'base.html',
            page_name='prev 24hr stats',
            page_html=html
            )


@app.route('/hostname_progress')
def hostname_progress():
    tld=''
    if request.args.get('tld') is not None:
        tld = "and substring(hostname from '\.[^\.]+$') = :tld"

    sql=text(f'''
    select *
    from hostname_progress
    where
        hostname is not null
        {tld}
    order by fraction_requested desc,num_frontier desc limit 10000
    ''')
    res=g.connection.execute(sql,{'tld':request.args.get('tld')})
    def callback(k,v):
        if k=='hostname':
            return f'<a href=/hostname/{v}>{v}</a>'
    html = res2html(res,callback)

    return render_template(
            'base.html',
            page_name='hostname progress',
            page_html=html
            )


@app.route('/hostname_productivity/<tld>')
def hostname_productivity_tld(tld):
    return hostname_productivity(tld)


@app.route('/hostname_productivity')
def hostname_productivity(tld=None):
    where_tld=''
    if tld is not None:
        where_tld = "and substring(hostname from '\.[^\.]+$') = :tld"

    limit = get_alphanum('limit', default=100)
    order_by = get_alphanum('order_by', default='valid_keyword_fraction')

    sql=text(f'''
    select *
    from hostname_productivity
    where
        hostname is not null
        {where_tld}
    order by {order_by} desc
    limit :limit
    ;
    ''')
    res=g.connection.execute(sql, {
        'tld':tld,
        'order_by':order_by,
        'limit':limit,
        })
    def callback(k,v,is_header):
        if is_header:
            style = ''
            if order_by==v:
                style = f'style="font-style: italic;"'
            html=f'<a href=?order_by={v} {style}>{v}</a>'
            return html
        else:
            if k=='hostname':
                return f'<a href=/hostname/{v}>{v}</a>'
    html = res2html(res,callback)

    return render_template(
            'base.html',
            page_name='hostname productivity',
            page_html=html
            )


@app.route('/hostname_articles/<hostname>/<year>')
def articles_hostname_year(hostname,year):
    html=''

    if year=='undefined':
        extract_clause='and extract (year from pub_time) is :year'
        year=None
    else:
        extract_clause='and extract (year from pub_time) = :year'

    if request.args.get('keywords') is None:
        keywords_where=''
    else:
        keywords_where='and (num_title>0 or num_text>0)'

    sql=text(f'''
    select
        pub_time,
        lang,
        articles.id_articles,
        id_urls = (CASE
            WHEN articles.id_urls_canonical = 2425
            THEN articles.id_urls
            ELSE articles.id_urls_canonical
            END) as canonical,
        (num_title>0 or num_text>0) as keyword,
        title
    from articles
    inner join keywords on keywords.id_articles = articles.id_articles
    where
        hostname = :hostname
        {extract_clause}
        {keywords_where}
    order by pub_time desc
    limit 100;
    ''')
    res=g.connection.execute(sql,{'hostname':hostname,'year':year})
    def callback(k,v):
        if k=='id_articles':
            return f'<a href=/article/{v}>{v}</a>'
    html+=res2html(res,callback)

    return render_template(
        'base.html',
        page_name=f'{hostname}, year = {year}',
        page_html=html
        )


@app.route('/recent_urls')
def recent_urls():
    html=''

    sql=text('''
    select id_urls
    from articles
    order by id_articles desc
    limit 10000;
    ''')
    res=g.connection.execute(sql)
    for row in res:
        url = urlinfo2url(get_url_info_from_id_urls(g.connection,row['id_urls']))
        html+=f'<p><a href={url}>{url}</a></p>'
    #def callback(k,v):
        #if k=='id_urls':
            #url = urlinfo2url(get_url_info_from_id_urls(g.connection,v))
            #return f'<a href={url}>{url}</a>'
    #html+=res2html(res,callback)

    return render_template(
        'base.html',
        page_name=f'recent_urls',
        page_html=html
        )


@app.route('/hostname/<hostname>')
def hostname(hostname):
    html=''

    html+='<h2>Productivity Statistics</h2>'
    sql=text(f'''
    select *
    from hostname_productivity
    where hostname=:hostname
    ''')
    res=g.connection.execute(sql,{'hostname':hostname})
    html+=res2html(res,transpose=True)

    html+='<h2>Recent Responses</h2>'
    sql=text(f'''
    select timestamp, num
    from responses_timestamp_hostname
    where hostname=:hostname
    order by timestamp desc
    limit 10;
    ''')
    res=g.connection.execute(sql,{'hostname':hostname})
    html+=f'<p><a href=/recent_urls/{hostname}>view recent urls</a></p>'
    html+=res2html(res)

    html+='<h2>Language Usage</h2>'
    sql=text(f'''
    select
        lang,
        round(((#num_distinct)/(1.0*total))::numeric,4) as fraction,
        (#num_distinct)::int as num_distinct
    from articles_lang
    inner join (
        select
            hostname,
            sum(#num_distinct) as total
        from articles_lang
        where
            hostname=:hostname
        group by hostname
        ) as t1 on articles_lang.hostname = t1.hostname
    where articles_lang.hostname=:hostname
    order by num_distinct desc;
    ''')
    res=g.connection.execute(sql,{'hostname':hostname})
    html+=res2html(res)

    html+='<h2>Articles per Year</h2>'
    sql=text(f'''
    SELECT *
    FROM hostname_peryear
    WHERE hostname=:hostname
    ORDER BY year desc;
    ''')
    res=g.connection.execute(sql,{'hostname':hostname})
    def callback(k,v):
        if k=='year':
            return f'<a href=/hostname_articles/{hostname}/{v.strip()}?keywords=true>{v}</a>'
    html+=res2html(res,callback)

    return render_template(
            'base.html',
            page_name=hostname,
            page_html=html
            )


@app.route('/article/<id_articles>')
def article(id_articles):
    html=''

    # FIXME: add an index for authors
    """
    sql=text(f'''
    select author
    from authors
    where id_articles=:id_articles
        ;
    ''')
    res = g.connection.execute(sql,{'id_articles':id_articles})
    html+=f'<strong>authors:</strong>{list(res)}<br>'
    """

    sql=text(f'''
    select title,lang,pub_time,text,html,id_urls
    from articles
    where id_articles=:id_articles
        ;
    ''')
    res = g.connection.execute(sql,{'id_articles':id_articles})
    row = res.first()

    url = urlinfo2url(get_url_info_from_id_urls(g.connection,row['id_urls']))
    html+=f'<strong>pub_time:</strong> {row["pub_time"]}<br>'
    html+=f'<strong>lang:</strong> {row["lang"]}<br>'
    html+=f'<strong>link:</strong> <a href={url}>{url}</a><br>'

    html+='<div style="width:800px;">'


    if row['html'] is None:
        article_orig=''
        article_orig+='<pre style="overflow-x: auto; word-wrap: break-word; white-space: pre-wrap;">'
        article_orig+=row['text']
        article_orig+='</pre>'
    else:
        article_orig = row['html']

    query = request.args.get('query')
    if query is not None:

        COLOR = ['red', 'blue', 'orange', 'violet', 'green','lightred','lightblue','purple','lightgreen']

        query_words = query.replace('&',' ').replace('|',' ').replace('(',' ').replace(')',' ').split()
        #regex = re.compile('|'.join([r'(\b'+word+r'\b)' for word in query_words]), re.I)
        regex = re.compile('|'.join([r'('+word+r')' for word in query_words]), re.I)

        # FIXME: use bs4
        i = 0;
        article_mod=''
        for m in regex.finditer(article_orig):
            article_mod += "".join([
                article_orig[i:m.start()],
                "<strong><span style='color:%s'>" % COLOR[m.lastindex-1],
                article_orig[m.start():m.end()],
                "</span></strong>"
                ])
            i = m.end()
        article_mod += article_orig[m.end():]
        html += article_mod
    else:
        html += article_orig

    html+='</div>'

    return render_template(
            'base.html',
            page_name=row['title'],
            page_html=html
            )


if __name__ == "__main__":
    #app.run(host='0.0.0.0', port=8000)
    app.run(host='0.0.0.0', port=8080)
    #app.run(host='10.253.1.15', port=8000)
    #app.run(host='10.253.1.15', port=30000)
