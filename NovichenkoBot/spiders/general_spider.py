import scrapy
from scrapy import Spider
from scrapy.http import Request
from scrapy.http.response.html import HtmlResponse
from scrapy.linkextractors import LinkExtractor

from bs4 import BeautifulSoup
import sqlalchemy
from sqlalchemy.sql import text
import datetime
from dateutil.parser import parse
import langid
import newspaper 

from NovichenkoBot.sqlalchemy_utils import get_url_info

class GeneralSpider(Spider):
    name = 'general'

    def __init__(
            self, 
            db='postgres:///novichenkobot', 
            connection=None,
            keywords='inputs/keywords.txt',
            *args, 
            **kwargs
            ):
        super(GeneralSpider, self).__init__(*args, **kwargs)
        self.le = LinkExtractor()

        # load keywords dictionaries
        self.keywords={}
        with open(keywords) as f:
            for line in f:
                lang,wordstr=line.split(':')
                words=[word.strip() for word in wordstr.split(',')]
                self.keywords[lang]=words

        # langid lazily loads models for language identification,
        # and calling it here forces it to load the models now
        lang=langid.classify('test')[0]

        # database connection
        if connection is None:
            engine = sqlalchemy.create_engine(db, connect_args={'connect_timeout': 120})
            self.connection = engine.connect()
        else:
            self.connection = connection

    def parse(self, response):

        # only parse html pages
        if not isinstance(response, HtmlResponse):
            return
        
        # process the downloaded webpage
        article=html2article(response.url,response.body)
        all_links=self.le.extract_links(response)

        # insert article into database
        with self.connection.begin() as trans:

            # insert into articles table
            url_info=get_url_info(
                    self.connection,
                    article.canonical_link,
                    depth=response.request.depth,
                    )
            id_urls_canonical=url_info['id_urls']
            sql=sqlalchemy.sql.text('''
                INSERT INTO articles 
                (id_urls,hostname,id_urls_canonical,id_responses,title,text,html,lang,pub_time) 
                values 
                (:id_urls,:hostname,:id_urls_canonical,:id_responses,:title,:text,:html,:lang,:pub_time)
                returning id_articles
            ''')
            res=self.connection.execute(sql,{
                'id_urls':response.request.id_urls,
                'hostname':response.request.hostname,
                'id_urls_canonical':id_urls_canonical,
                'id_responses':response.id_responses,
                'title':article.title,
                'text':article.text,
                'html':article.article_html,
                'lang':article.lang,
                'pub_time':article.publish_date,
                })
            id_articles=res.first()[0]

            # update keywords table
            keywords_lang=self.keywords.get(article.lang,[])
            alltext_lower=article.alltext.lower()
            text_lower=article.text.lower()
            title_lower=article.title.lower()
            keywords_alltext=sum([ alltext_lower.count(keyword) for keyword in keywords_lang])
            keywords_text=sum([ text_lower.count(keyword)       for keyword in keywords_lang])
            keywords_title=sum([ title_lower.count(keyword)     for keyword in keywords_lang])
            sql=sqlalchemy.sql.text('''
            INSERT INTO keywords
                (id_articles,keyword,num_title,num_text,num_alltext)
                VALUES
                (:id_articles,:keyword,:num_title,:num_text,:num_alltext)
            ''')
            res=self.connection.execute(sql,{
                'id_articles':id_articles,
                'keyword':'north korea',
                'num_title':keywords_title,
                'num_text':keywords_text,
                'num_alltext':keywords_alltext,
                })

            # update authors table
            for author in article.authors:
                sql=sqlalchemy.sql.text('''
                    INSERT INTO authors (id_articles,author) values (:id_articles,:author)
                ''')
                self.connection.execute(sql,{
                    'id_articles':id_articles,
                    'author':author,
                    })

            # update refs table
            refs=[]
            refs.append([article.top_image,'top_image',''])
            for url in article.images:
                refs.append([url,'image',''])
            for url in article.movies:
                refs.append([url,'movie',''])
            for url in all_links:
                refs.append([url.url,'link',url.text])
            for (url,url_type,text) in refs:
                target_url_info=get_url_info(
                    self.connection,
                    url,
                    depth=response.request.depth+1
                    )
                if target_url_info is not None:
                    target=target_url_info['id_urls']
                    sql=sqlalchemy.sql.text('''
                    insert into refs
                        (source,target,type,text)
                        values
                        (:source,:target,:type,:text);
                    ''')
                    self.connection.execute(sql,{
                        'source':id_articles,
                        'target':target,
                        'type':url_type,
                        'text':text,
                        })

        # yield all links
        for link in all_links:
            r = scrapy.http.Request(url=link.url)
            r.priority = 100*keywords_title+keywords_text+keywords_alltext
            r.meta.update(link_text=link.text)
            r.depth=response.request.depth+1
            yield r

def html2article(url,html):
    '''
    extract article information from html source;
    this function mostly relies on the newspaper3k library to extract information,
    but some hostnames require their own additional parsing code
    '''

    # detect page language 
    soup=BeautifulSoup(html,'lxml')
    alltext=soup.get_text(separator=' ')
    # FIXME: can we make things faster?
    # Some pages have serious bottlenecks, but we don't know what they are.
    # alltext=soup.find('body').get_text(separator=' ')[:10000]
    lang=langid.classify(alltext)[0]

    # extract content using newspaper3k library;
    # newspaper3k is able to automatically detect the language sometimes,
    # but their technique is not robust because it requires the language to be specified in the html,
    # and it fails silently if it is unable to detect a langauge;
    # therefore, we first try with the langauge detected above,
    # and only if that fails do we rely on newspaper3k's implementation
    try:
        article = newspaper.Article(url,keep_article_html=True,MAX_TEXT=None,language=lang)
        article.download(input_html=html)
        article.parse()
    except:
        article = newspaper.Article(url,keep_article_html=True,MAX_TEXT=None)
        article.download(input_html=html)
        article.parse()

    article.lang=lang
    article.alltext=alltext

    # for some domains, newspaper3k doesn't work well
    # and so we have manual rules for extracting information
    if article.publish_date is None:
        try:
            article.publish_date=parse(soup.find('time').text)
        except:
            pass

    try:
        if 'https://angrystaffofficer.com' in url:
            article.authors=soup.find('span',class_='author vcard').find('a').text.split('and')

        if 'https://www.armscontrol.org' in url:
            article.authors=soup.find('a',href='#bio').text.split('and')

        if 'armscontrolwonk.com' in url:
            article.publish_date=parse(soup.find('span',class_='date published time').text)

        if 'https://www.bbc.com' in url:
            article.authors=['BBC']
            article.publish_date=parse(soup.find('div',class_='date').text)

        if 'www.csis.org' in url:
            article.publish_date=parse(soup.find('article',role='article').find('p').text)

        if 'www.dailynk.com' in url:
            article.authors=soup.find('div',class_='td-post-author-name').find('a').text.split('and')

        if 'elperuano.pe' in url:
            article.publish_date=parse(soup.find('article',class_='notatexto').find('b').text)
            article.authors=soup.find('article',class_='notatexto').find(['strong','em']).text.split('and')

        # FIXME: parser can't handle spanish dates
        #if 'elnacional.com.do' in url:
            #article.publish_date=parse(soup.find('div',class_='post-meta-data').find_all('p',class_='meta-item-details')[0].text)
            #article.authors=soup.find('div',class_='post-meta-data').find_all('p',class_='meta-item-details')[1].text.split('and')

        if 'foxnews.com' in url:
            article.publish_date=parse(soup.find('div',class_='article-date').find('time').text)

        if 'www.infowars.com' in url:
            article.authors=soup.find('span',class_='author').text.split('and')

        if 'janes.com' in url:
            article.publish_date=parse(soup.find('div',class_='date').text)
            article.authors=soup.find('div',class_='byline').find('b').text.split(',')[0].split('and')

        if 'www.newsweek.com' in url:
            article.authors=soup.find('span',class_='author').text.split('and')

        if 'www.northkoreatech.org' in url:
            article.authors=soup.find('span',class_='entry-meta-author vcard author').text.split(':')[-1].split('and')

        if 'www.nkleadershipwatch.org' in url or 'nkleadershipwatch.wordpress.com' in url:
            article.authors=['__NOAUTHOR__']

        if 'peru21.pe' in url:
            article.authors=['peru21']

        if 'politico.eu' in url:
            article.publish_date=parse(soup.find('p',class_='timestamp').find('time')['datetime'])
            article.authors=soup.find('span',class_='byline').text.split('and')

        if 'www.stripes.com' in url:
            credits=soup.find('div',class_='article_credits')
            article.authors=credits.text.split('|')[0][3:].split('and')
            article.publish_date=parse(credits.text.split(':')[1])

        if 'thediplomat.com' in url:
            article.publish_date=parse(soup.find('span',itemprop='datePublished').text)
            article.authors=soup.find('div',class_='td-author').find('strong').text.split('and')

        if 'https://time.com' in url:
            #article.authors=soup.find('a',class_='author-name').text.split('and')
            article.publish_date=parse(soup.find('div',class_='published-date').text)

        if 'https://thehill.com' in url:
            article.authors=soup.find('span',class_='submitted-by').find('a').text.split('and')

        if 'usatoday.com' in url:
            article.authors=soup.find('a',class_='gnt_ar_by_a').text.split('and')

        if 'www.usnews.com' in url:
            # FIXME: can we even download www.usnews.com webpages?
            # These two are giving permissions errors
            # https://www.usnews.com/news/world/articles/2019-12-05/north-korea-threatens-to-resume-calling-trump-dotard
            # https://www.usnews.com/news/world-report/articles/2019-11-06/north-korea-threatens-to-upend-nuclear-talks-due-to-us-reckless-military-frenzy
            article.authors=soup.find('a',class_='Anchor-s1mkgztv-0').text.split('and')

    except AttributeError:
        pass

    return article
