import scrapy
from scrapy import Spider
from scrapy.http import Request
from scrapy.http.response.html import HtmlResponse
from scrapy.linkextractors import LinkExtractor

from urllib.parse import urlparse
from bs4 import BeautifulSoup
import sqlalchemy
import datetime
import langid
import newspaper 

from NovichenkoBot.sqlalchemy_utils import get_url_info

class GeneralSpider(Spider):
    name = 'general'

    def __init__(
            self, 
            db='sqlite:///benchmark.db', 
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
        self.engine = sqlalchemy.create_engine(db, connect_args={'timeout': 120})
        self.connection = self.engine.connect()

    def parse(self, response):

        # only parse html pages
        if not isinstance(response, HtmlResponse):
            return
        
        # basic webpage information
        domain=urlparse(response.url).hostname
        all_links=self.le.extract_links(response)

        # detect page language 
        soup=BeautifulSoup(response.body,'lxml')
        alltext=soup.get_text(separator=' ')
        lang=langid.classify(alltext)[0]

        # extract content 
        if response.body==b'':
            article = newspaper.Article(response.url)
            article.title=''
            article.text=''
        else:
            try:
                article = newspaper.Article(response.url,language=lang)
                article.download(input_html=response.body)
                article.parse()
            except:
                article = newspaper.Article(response.url)
                article.download(input_html=response.body)
                article.parse()

        # update database
        with self.connection.begin() as trans:
            url_info=get_url_info(
                    self.connection,
                    article.canonical_link,
                    depth=response.request.depth,
                    )
            id_urls_canonical=url_info['id_urls']
            res=self.connection.execute('''
                INSERT INTO articles 
                (id_urls,id_urls_canonical,id_responses,title,alltext,text,lang,pub_time) 
                values 
                (?,?,?,?,?,?,?,?)
                ''',(
                response.request.id_urls,
                id_urls_canonical,
                response.id_responses,
                article.title,
                alltext,
                article.text,
                lang,
                article.publish_date,
                ))
            id_articles=res.lastrowid

            # update keywords table
            keywords_lang=self.keywords.get(lang,[])
            alltext_lower=alltext.lower()
            text_lower=article.text.lower()
            title_lower=article.title.lower()
            keywords_alltext=sum([ alltext_lower.count(keyword) for keyword in keywords_lang])
            keywords_text=sum([ text_lower.count(keyword)       for keyword in keywords_lang])
            keywords_title=sum([ title_lower.count(keyword)     for keyword in keywords_lang])
            res=self.connection.execute('''
                INSERT INTO keywords
                    (id_articles,keyword,num_title,num_text,num_alltext)
                    VALUES
                    (?,?,?,?,?)
                ''',(
                    id_articles,
                    'north korea',
                    keywords_title,
                    keywords_text,
                    keywords_alltext,
                ))

            # update authors table
            for author in article.authors:
                self.connection.execute('INSERT INTO authors (id_articles,author) values (?,?)',(
                    id_articles,
                    author,
                    ))

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
                target=get_url_info(self.connection,url,depth=response.request.depth+1)['id_urls']
                self.connection.execute('''
                    insert into refs
                        (source,target,type,text)
                        values
                        (?,?,?,?);
                    ''',(
                    id_articles,
                    target,
                    url_type,
                    text
                    ))
        
        # yield all links
        for link in all_links:
            r = scrapy.http.Request(url=link.url)
            r.priority = 100*keywords_title+keywords_text+keywords_alltext
            r.meta.update(link_text=link.text)
            r.depth=response.request.depth+1
            yield r
