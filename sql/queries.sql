--------------------------------------------------------------------------------
-- the following pairs of queries should all give the same results,
-- but the second query uses the cache table to greatly speed up the calculation
--------------------------------------------------------------------------------

-- get total articles per domain

select domain,count(1) from articles group by domain order by count(1);

select domain,sum(num_articles) from articles_domains group by domain order by sum(num_articles);

-- get articles for a domain by year

select domain,strftime("%Y",pub_time) as year,count(1) from articles where domain='tsl.news' group by domain,year;

select domain,year,num_keywords from articles_domains where domain='www.granma.cu';

-- get pagerank summary stats

select domain_source,domain_target,count(1) from refs where type='link' group by domain_source,domain_target order by domain_source,count(1);

select * from refs_domains order by domain_source,num_refs,domain_target;

--------------------------------------------------------------------------------
-- the following queries calculate interesting properties
--------------------------------------------------------------------------------

-- get articles per year grouping common domains
select replace(domain,'www.','') as d,year,sum(num_keywords) from articles_domains group by d,year;

-- find domains linking to 38north.org
select * from refs_domains where domain_target like '%38north.org' and type='link' order by num_refs;

-- gets pagerank stats excluding target domains that are not included in sources
select t1.domain_source,t1.domain_target,t1.num_refs from refs_domains as t1 where t1.type='link' and t1.domain_target in (select domain_source from refs_domains) order by t1.domain_source,t1.num_refs;

select 
    replace(t1.domain_source,'www.','') as ds,
    replace(t1.domain_target,'www.','') as dt,
    sum(t1.num_refs) as nr
from refs_domains as t1
where
    t1.type='link' and
    dt in (select replace(domain_source,'www.','') as ds from refs_domains group by ds)
order by ds,nr;


-- displays linked domains that appear on less than 50% of pages
-- this is a heuristic for the linked domains that appear in the body of a webpage,

select domain_source,domain_target,num_refs,CAST (num_refs AS REAL)/a.num_articles as percent
from refs_domains 
left join 
    ( SELECT domain,sum(num_articles) as num_articles from articles_domains group by domain
    ) AS a 
    on refs_domains.domain_source=a.domain 
    where percent<0.5
    order by domain_source,num_refs,domain_target;

-- this query is similar to the above, but it focuses on target urls instead of domains

select domain_source,url,num_urls,CAST (num_urls AS REAL)/a.num_articles as percent
from refs_urls
left join 
    ( SELECT domain,sum(num_articles) as num_articles from articles_domains group by domain
    ) AS a 
    on refs_urls.domain_source=a.domain 
    where percent<0.1
    order by domain_source,num_urls;

-- again, similar to above but then regroups on domains
-- this should give similar results to the first one, 
-- except it will also include links to articles on the root domain 
-- (that appear in the bodies of articles and not menus)

select domain_source,domain_target,sum(num_urls),CAST (num_urls AS REAL)/a.num_articles as percent
from refs_urls
left join 
    ( SELECT domain,sum(num_articles) as num_articles from articles_domains group by domain
    ) AS a 
    on refs_urls.domain_source=a.domain 
    where percent<0.1
    group by domain_target
    order by domain_source,num_urls;
