# -*- coding: utf-8 -*-
BOT_NAME = 'NovichenkoBot'

SPIDER_MODULES = ['NovichenkoBot.spiders']
NEWSPIDER_MODULE = 'NovichenkoBot.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'NovichenkoBot'
#USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'

SCHEDULER = 'NovichenkoBot.sqlalchemy_scheduler.Scheduler'
#SCHEDULER = 'frontera.contrib.scrapy.schedulers.frontier.FronteraScheduler'

SPIDER_MIDDLEWARES = {
    #'frontera.contrib.scrapy.middlewares.schedulers.SchedulerSpiderMiddleware': 1000,
    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': None,
    'scrapy.spidermiddlewares.depth.DepthMiddleware': None,
    'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': None,
    'scrapy.spidermiddlewares.referer.RefererMiddleware': None,
    'scrapy.spidermiddlewares.urllength.UrlLengthMiddleware': None
}

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': 100,
    'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': None,
    'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware': 350,
    'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': 400,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 500,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    'scrapy.downloadermiddlewares.ajaxcrawl.AjaxCrawlMiddleware': None,
    'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': None, #580,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 590,
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None, #600,
    'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 700,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
    'scrapy.downloadermiddlewares.stats.DownloaderStats': 850,
    'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 900,
    #'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': None,
    #'NovichenkoBot.sqlalchemy_downloader.Downloader':1000,
    #'frontera.contrib.scrapy.middlewares.schedulers.SchedulerDownloaderMiddleware': 1000,
}
#{
    #'scrapy.spidermiddlewares.referer.RefererMiddleware': 700,
    #'scrapy.spidermiddlewares.urllength.UrlLengthMiddleware': 800,
    #'scrapy.spidermiddlewares.depth.DepthMiddleware': 900,
#}

TELNETCONSOLE_ENABLED = False
ROBOTSTXT_OBEY = True
DUPEFILTER_CLASS = 'scrapy.dupefilters.BaseDupeFilter'

DOWNLOAD_FAIL_ON_DATALOSS = False
HTTPCACHE_ENABLED = False
REDIRECT_ENABLED = True
COOKIES_ENABLED = False
DOWNLOAD_TIMEOUT = 300
RETRY_ENABLED = False
DOWNLOAD_MAXSIZE = 10*1024*1024

# auto throttling
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_DEBUG = False
AUTOTHROTTLE_MAX_DELAY = 30.0
AUTOTHROTTLE_START_DELAY = 0.5
RANDOMIZE_DOWNLOAD_DELAY = False

# concurrency
#CONCURRENT_REQUESTS = 256
CONCURRENT_REQUESTS = 128
CONCURRENT_REQUESTS_PER_DOMAIN = 64
DOWNLOAD_DELAY = 0.0

LOG_LEVEL = 'INFO'

REACTOR_THREADPOOL_MAXSIZE = 32
#REACTOR_THREADPOOL_MAXSIZE = 16
DNS_TIMEOUT = 240
FRONTERA_SETTINGS = 'config.spider'
HTTPERROR_ALLOW_ALL = True
