import sqlalchemy

import scrapy
from scrapy import Spider
from scrapy.http import Request
from scrapy.http.response.html import HtmlResponse
from scrapy.linkextractors import LinkExtractor

class Benchmark(Spider):
    name = 'benchmark'

    def __init__(
            self, 
            db='sqlite:///benchmark.db', 
            *args, 
            **kwargs
            ):
        super(Benchmark, self).__init__(*args, **kwargs)
        self.le = LinkExtractor()

        # database connection
        #self.engine = sqlalchemy.create_engine(db, connect_args={'timeout': 120})
        self.engine = sqlalchemy.create_engine(db, connect_args={'connect_timeout': 120})
        self.connection = self.engine.connect()

    def parse(self, response):

        # only parse html pages
        if not isinstance(response, HtmlResponse):
            return

        # basic webpage information
        all_links=self.le.extract_links(response)
        
        # yield all links
        for link in all_links:
            r = scrapy.http.Request(url=link.url)
            r.meta.update(link_text=link.text)
            r.depth=response.request.depth+1
            yield r
