from flask import Flask, g, url_for, render_template, request
import sqlalchemy
from sqlalchemy.sql import text
import time

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_url_info_from_id_urls, urlinfo2url

app = Flask(__name__)


@app.before_request
def before_request():
    g.start = time.time()
    db = 'postgres:///novichenkobot'
    engine = sqlalchemy.create_engine(db)
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


def res2html(res,col_formatter=None,transpose=False,click_headers=False):
    rows=[list(res.keys())]+list(res)
    if transpose:
        rows=list(map(list, zip(*rows)))
    html='<table>'
    for i,row in enumerate(rows):
        html+='<tr>'
        if i==0 and not transpose:
            td='th'
            html+='<th></th>'
        else:
            td='td'
            html+=f'<td>{i}</td>'
        for j,col in enumerate(row):
            val = None
            if i>0 and col_formatter is not None:
                val = col_formatter(res.keys()[j],col)
            elif i==0 and click_headers:
                val = f'<a href="?order_by={col}">{col}</a>'
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
            return f'<a href=/hostname_productivity?tld={v}>{v}</a>'
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
    order_by=request.args.get('order_by')
    if order_by is None:
        order_by='fraction_lang'
    order_by=order_by[:20]

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
    ORDER BY {order_by} {order_dir};
    ''')
    res=g.connection.execute(sql,{'lang':lang,'order_by':order_by,'order_dir':order_dir})
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
        page_html=html
        )


@app.route('/hostname_productivity')
def hostname_productivity():
    tld=''
    if request.args.get('tld') is not None:
        tld = "and substring(hostname from '\.[^\.]+$') = :tld"
    
    sql=text(f'''
    select *
    from hostname_productivity 
    where 
        hostname is not null
        {tld}
    ;
    ''')
    res=g.connection.execute(sql,{'tld':request.args.get('tld')})
    def callback(k,v):
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
        extract_clause='extract (year from pub_time) is :year'
        year=None
    else:
        extract_clause='extract (year from pub_time) = :year'
    sql=text(f'''
    select 
        pub_time,
        lang,
        id_articles,
        id_urls = (CASE 
            WHEN articles.id_urls_canonical = 2425 
            THEN articles.id_urls 
            ELSE articles.id_urls_canonical 
            END) as canonical,
        title
    from articles
    where 
        hostname = :hostname and
        {extract_clause}
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
    SELECT
        CASE WHEN t1.year = '-inf' THEN 'undefined' ELSE to_char(t1.year,'0000') END as year,
        num :: int,
        num_distinct :: int,
        CASE WHEN num_distinct_keyword IS NULL THEN 0 ELSE num_distinct_keyword END :: int,
        round((CASE WHEN num_distinct_keyword IS NULL THEN 0 ELSE num_distinct_keyword END / num_distinct) :: numeric,4) as keyword_fraction
    FROM (
        SELECT
            hostname,
            extract(year from day) as year,
            sum(num) as num,
            sum(#num_distinct) as num_distinct
        FROM articles_summary2
        WHERE hostname=:hostname
        GROUP BY hostname,year
    ) AS t1
    LEFT JOIN (
        SELECT
            hostname,
            extract(year from day) as year,
            sum(#num_distinct) as num_distinct_keyword
        FROM articles_summary2
        WHERE hostname=:hostname AND keyword=true
        GROUP BY hostname,year
    ) AS t2 on t1.hostname = t2.hostname and t1.year = t2.year
    ORDER BY year DESC;
    ''')
    sql=text(f'''
    SELECT * 
    FROM hostname_peryear
    WHERE hostname=:hostname;
    ''')
    res=g.connection.execute(sql,{'hostname':hostname})
    def callback(k,v):
        if k=='year':
            return f'<a href=/hostname_articles/{hostname}/{v.strip()}>{v}</a>'
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
        html+='<pre style="overflow-x: auto; word-wrap: break-word; white-space: pre-wrap;">'
        html+=row['text']
        html+='</pre>'
    else:
        html+=row['html']
    html+='</div>'

    return render_template(
            'base.html',
            page_name=row['title'],
            page_html=html
            )


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
