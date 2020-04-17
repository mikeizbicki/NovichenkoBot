/*
 * Views on articles_lang
 */
CREATE MATERIALIZED VIEW articles_lang_hostnames AS
SELECT DISTINCT hostname FROM articles_lang;
CREATE UNIQUE INDEX articles_lang_hostnames_index ON articles_lang_hostnames(hostname);

CREATE MATERIALIZED VIEW articles_lang_stats AS
SELECT
    lang,
    sum(#num_distinct) as num_distinct
FROM articles_lang
GROUP BY lang;
CREATE UNIQUE INDEX articles_lang_stats_index ON articles_lang_stats(lang);

/*
 * Views on responses_summary
 */

CREATE MATERIALIZED VIEW total_bytes AS
SELECT pg_size_pretty(sum(bytes)) as total_bytes FROM responses_summary;
CREATE UNIQUE INDEX total_bytes_index on total_bytes(total_bytes);

/*
 * Views on responses_timestamp_hostname
 */

CREATE MATERIALIZED VIEW responses_timestamp_hostname_hostnames AS
SELECT DISTINCT hostname FROM responses_timestamp_hostname;
CREATE UNIQUE INDEX responses_timestamp_hostname_hostnames_index ON responses_timestamp_hostname_hostnames(hostname);

CREATE MATERIALIZED VIEW responses_timestamp_hostname_recent_1hr AS
SELECT hostname,num
FROM responses_timestamp_hostname 
WHERE timestamp = date_trunc('hour',now()) - interval '1 hour'
ORDER BY num DESC;
CREATE UNIQUE INDEX responses_timestamp_hostname_recent_index_1hr ON responses_timestamp_hostname_recent_1hr(hostname);

CREATE MATERIALIZED VIEW responses_timestamp_hostname_recent AS
SELECT hostname,sum(num) AS num 
FROM responses_timestamp_hostname 
WHERE timestamp > now() - interval '1 day' 
GROUP BY hostname 
ORDER BY num DESC;
CREATE UNIQUE INDEX responses_timestamp_hostname_recent_index ON responses_timestamp_hostname_recent(hostname);

/*
 * Views on refs_summary
 */

CREATE MATERIALIZED VIEW refs_summary_simple AS
SELECT
    hostname_source,
    hostname_target,
    sum(num_all) :: int as num_all,
    sum(num_keywords) :: int as num_keywords,
    (#hll_union_agg(distinct_all)) :: int as distinct_all,
    (#hll_union_agg(distinct_keywords)) :: int as distinct_keywords
FROM refs_summary
WHERE
    type='link'
GROUP BY hostname_source,hostname_target
ORDER BY hostname_source,hostname_target;
CREATE UNIQUE INDEX refs_summary_simple_index ON refs_summary_simple(hostname_source,hostname_target);

/*
 * Views on articles_summary2
 */
CREATE MATERIALIZED VIEW hostname_progress AS
SELECT
    frontier_hostname.hostname,
    COALESCE(frontier_hostname.num,0) as num_frontier,
    COALESCE(requests_hostname.num,0) as num_requests,
    COALESCE(ROUND(requests_hostname.num/frontier_hostname.num::numeric,4),0) as fraction_requested,
    COALESCE(responses_hostname.num,0) as num_responses,
    COALESCE(ROUND(responses_hostname.num/requests_hostname.num::numeric,4),0) as fraction_responded,
    COALESCE(articles_hostname.num,0) as num_articles,
    COALESCE(ROUND(articles_hostname.num/responses_hostname.num::numeric,4),0) as fraction_articles,
    COALESCE(articles_hostname_keyword.num,0) as num_articles_keyword,
    COALESCE(ROUND(articles_hostname_keyword.num/articles_hostname.num::numeric,4),0) as fraction_keyword
FROM frontier_hostname
LEFT OUTER JOIN requests_hostname ON requests_hostname.hostname=frontier_hostname.hostname
LEFT OUTER JOIN (
    SELECT hostname,sum(num) as num
    FROM responses_hostname
    GROUP BY hostname
) responses_hostname ON frontier_hostname.hostname=responses_hostname.hostname
LEFT OUTER JOIN (
    SELECT hostname,sum(num) as num
    FROM articles_summary2 
    WHERE keyword=FALSE 
    GROUP BY hostname
) articles_hostname ON frontier_hostname.hostname=articles_hostname.hostname
LEFT OUTER JOIN (
    SELECT hostname,sum(num) as num
    FROM articles_summary2 
    WHERE keyword=TRUE 
    GROUP BY hostname
) articles_hostname_keyword ON frontier_hostname.hostname=articles_hostname_keyword.hostname
;
CREATE UNIQUE INDEX hostname_progress_index ON hostname_progress(hostname);
CREATE INDEX hostname_progress_index2 ON hostname_progress(fraction_requested);

CREATE MATERIALIZED VIEW hostname_productivity AS
SELECT
    COALESCE(t1v.hostname,t2v.hostname,t1.hostname,t2.hostname) as hostname,
    COALESCE(t1v.num_distinct_keywords::int,0) AS valid_keywords,
    COALESCE(t2v.num_distinct_total::int,0) as valid_total,
    COALESCE(ROUND((t1v.num_distinct_keywords/t2v.num_distinct_total)::numeric,4),0) as valid_keyword_fraction,
    COALESCE(t1.num_distinct_keywords::int,0) AS all_keywords,
    COALESCE(t2.num_distinct_total::int,0) as all_total,
    COALESCE(ROUND((t1.num_distinct_keywords/t2.num_distinct_total)::numeric,4),0) as all_keyword_fraction,
    COALESCE(ROUND((t2v.num_distinct_total/t2.num_distinct_total)::numeric,4),0) as valid_fraction,
    COALESCE(ROUND((t1v.num_distinct_keywords*(t1v.num_distinct_keywords/t2v.num_distinct_total))::numeric,4),0) as score,
    COALESCE(ROUND((t1.num_distinct_keywords/t2.num_distinct_total/sqrt(t2.num_distinct_total) )::numeric,4),0) as priority
FROM (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_total
    FROM articles_summary2
    GROUP BY hostname 
) t2 
FULL OUTER JOIN (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_keywords
    FROM articles_summary2
    WHERE 
        keyword=true 
    GROUP BY hostname 
) t1 ON t2.hostname = t1.hostname
FULL OUTER JOIN (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_total
    FROM articles_summary2
    WHERE 
        day!='-infinity'
    GROUP BY hostname 
) t2v ON t2v.hostname = t2.hostname
FULL OUTER JOIN (
    SELECT 
        hostname,
        sum(#num_distinct) as num_distinct_keywords
    FROM articles_summary2
    WHERE 
        keyword=true 
        AND day!='-infinity'
    GROUP BY hostname 
) t1v ON t1v.hostname = t2.hostname
ORDER BY priority DESC;
CREATE UNIQUE INDEX hostname_productivity_index ON hostname_productivity(hostname);

CREATE MATERIALIZED VIEW hostname_peryear AS
SELECT
    t1.hostname,
    CASE WHEN t1.year = '-inf' THEN 'undefined' ELSE to_char(t1.year,'0000') END as year,
    num :: int,
    num_distinct :: int,
    COALESCE(num_distinct_keyword,0)::int as num_distinct_keyword,
    round((COALESCE(num_distinct_keyword,0) / num_distinct) :: numeric,4) as keyword_fraction
FROM (
    SELECT
        hostname,
        extract(year from day) as year,
        sum(num) as num,
        sum(#num_distinct) num_distinct
    FROM articles_summary2
    GROUP BY hostname,year
) AS t1
LEFT JOIN (
    SELECT
        hostname,
        extract(year from day) as year,
        sum(#num_distinct) num_distinct_keyword
    FROM articles_summary2
    WHERE keyword
    GROUP BY hostname,year
) AS t2 on t1.hostname = t2.hostname and t1.year = t2.year
ORDER BY t1.hostname DESC,year DESC;
CREATE UNIQUE INDEX hostname_peryear_index ON hostname_peryear(hostname,year);

/*
 * Views for search
 */
CREATE MATERIALIZED VIEW search_corona AS
SELECT
    id_articles,
    lang,
    articles.hostname,
    pub_time,
    title,
    (   urls.scheme || '://' ||
        urls.hostname || path ||
        case when length(urls.params)=0 then '' else ';' || urls.params end ||
        case when length(urls.query)=0 then '' else '?' || urls.query end ||
        case when length(urls.fragment)=0 then '' else '#' || urls.fragment end
    ) as url
FROM articles 
INNER JOIN urls ON urls.id_urls = 
    (CASE WHEN articles.id_urls_canonical = 2425 
         THEN articles.id_urls 
         ELSE articles.id_urls_canonical END)
WHERE
    title &@~ '(bacteria) OR (bactérias) OR (bactéries) OR (bakteri) OR (bakterien) OR (bakterier) OR (batteri) OR (china virus) OR (china-virus) OR (chinese virus) OR (chinesisches virus) OR (cina virus) OR (corona) OR (coronavirus) OR (coronavírus) OR (covid) OR (covid-19) OR (cúm) OR (flu) OR (gribi) OR (grip) OR (gripe) OR (grippe) OR (gây) OR (gérmenes) OR (influensa) OR (influenza) OR (kina virus) OR (kinesiskt virus) OR (sars-cov-2) OR (trung quốc virus) OR (vi khuẩn) OR (virus) OR (virus chino) OR (virus chinois) OR (virus cina) OR (virus cinese) OR (virus de china) OR (virus de la porcelaine) OR (virus koroner) OR (virus porcellana) OR (virus wuhan) OR (virüs) OR (vírus) OR (vírus china) OR (vírus chinês) OR (vũ hán virus) OR (wuhan virus) OR (wuhan virüs) OR (wuhan vírus) OR (wuhan вирус) OR (wuhan-virus) OR (çin virüs) OR (çin virüsü) OR (Бактерий) OR (Бактерій) OR (Вирус) OR (Вірус) OR (ГРВІ-2) OR (Грип) OR (Гриппа) OR (Грипу) OR (Китай вірус) OR (Китайський вірус) OR (Корона) OR (Коронавирус) OR (Ухань вірус) OR (китай вирус) OR (китайский вирус) OR (коvid-19) OR (ковид-19) OR (коронавірус) OR (ווהאן וירוס) OR (וירוס) OR (וירוס סיני) OR (חיידקים) OR (נגיפי) OR (סארס-cov-2) OR (סין וירוס) OR (פעת) OR (קורונה) OR (שפעת) OR (آنفولانزا) OR (الانفلونزا) OR (البكتيريا) OR (السارس-cov-2) OR (الفيروس) OR (الفيروس التاجي) OR (الفيروس الصيني) OR (انفلوئنزا) OR (باکتری) OR (بیکٹیریا) OR (تاج) OR (خاورمیانه) OR (سآرس-cov-2) OR (سارس-2) OR (فلو) OR (فيروس الصين) OR (فيروس ووهان) OR (كورونا) OR (وائرس) OR (ووهان ویروس) OR (ووہان وائرس) OR (ویروس) OR (ویروس چین) OR (ویروس چینی) OR (چین وائرس) OR (چینی وائرس) OR (کوروناواروس) OR (インフルエンザ) OR (ウイルス) OR (コロナ) OR (コロナ ウイルス) OR (コヴィッド-19) OR (サルス・コヴ-2) OR (中国ウイルス) OR (中国病毒) OR (中國病毒) OR (冠 状 病毒) OR (冠 狀 病毒) OR (武汉病毒) OR (武漢ウイルス) OR (武漢病毒) OR (沙斯-cov-2) OR (流感) OR (电 晕) OR (病毒) OR (科維德-19) OR (科维德-19) OR (細菌) OR (细菌) OR (電 暈) OR (독감) OR (바이러스) OR (박테리아) OR (사스 코브-2) OR (우한 바이러스) OR (인플루엔자) OR (중국 바이러스) OR (중국어 바이러스) OR (코로나) OR (코로나 바이러스) OR (코비드-19)';

CREATE MATERIALIZED VIEW search_corona_text AS
SELECT
    id_articles,
    lang,
    articles.hostname,
    pub_time,
    title,
    (   urls.scheme || '://' ||
        urls.hostname || path ||
        case when length(urls.params)=0 then '' else ';' || urls.params end ||
        case when length(urls.query)=0 then '' else '?' || urls.query end ||
        case when length(urls.fragment)=0 then '' else '#' || urls.fragment end
    ) as url
FROM articles 
INNER JOIN urls ON urls.id_urls = 
    (CASE WHEN articles.id_urls_canonical = 2425 
         THEN articles.id_urls 
         ELSE articles.id_urls_canonical END)
WHERE
    text &@~ '(bacteria) OR (bactérias) OR (bactéries) OR (bakteri) OR (bakterien) OR (bakterier) OR (batteri) OR (china virus) OR (china-virus) OR (chinese virus) OR (chinesisches virus) OR (cina virus) OR (corona) OR (coronavirus) OR (coronavírus) OR (covid) OR (covid-19) OR (cúm) OR (flu) OR (gribi) OR (grip) OR (gripe) OR (grippe) OR (gây) OR (gérmenes) OR (influensa) OR (influenza) OR (kina virus) OR (kinesiskt virus) OR (sars-cov-2) OR (trung quốc virus) OR (vi khuẩn) OR (virus) OR (virus chino) OR (virus chinois) OR (virus cina) OR (virus cinese) OR (virus de china) OR (virus de la porcelaine) OR (virus koroner) OR (virus porcellana) OR (virus wuhan) OR (virüs) OR (vírus) OR (vírus china) OR (vírus chinês) OR (vũ hán virus) OR (wuhan virus) OR (wuhan virüs) OR (wuhan vírus) OR (wuhan вирус) OR (wuhan-virus) OR (çin virüs) OR (çin virüsü) OR (Бактерий) OR (Бактерій) OR (Вирус) OR (Вірус) OR (ГРВІ-2) OR (Грип) OR (Гриппа) OR (Грипу) OR (Китай вірус) OR (Китайський вірус) OR (Корона) OR (Коронавирус) OR (Ухань вірус) OR (китай вирус) OR (китайский вирус) OR (коvid-19) OR (ковид-19) OR (коронавірус) OR (ווהאן וירוס) OR (וירוס) OR (וירוס סיני) OR (חיידקים) OR (נגיפי) OR (סארס-cov-2) OR (סין וירוס) OR (פעת) OR (קורונה) OR (שפעת) OR (آنفولانزا) OR (الانفلونزا) OR (البكتيريا) OR (السارس-cov-2) OR (الفيروس) OR (الفيروس التاجي) OR (الفيروس الصيني) OR (انفلوئنزا) OR (باکتری) OR (بیکٹیریا) OR (تاج) OR (خاورمیانه) OR (سآرس-cov-2) OR (سارس-2) OR (فلو) OR (فيروس الصين) OR (فيروس ووهان) OR (كورونا) OR (وائرس) OR (ووهان ویروس) OR (ووہان وائرس) OR (ویروس) OR (ویروس چین) OR (ویروس چینی) OR (چین وائرس) OR (چینی وائرس) OR (کوروناواروس) OR (インフルエンザ) OR (ウイルス) OR (コロナ) OR (コロナ ウイルス) OR (コヴィッド-19) OR (サルス・コヴ-2) OR (中国ウイルス) OR (中国病毒) OR (中國病毒) OR (冠 状 病毒) OR (冠 狀 病毒) OR (武汉病毒) OR (武漢ウイルス) OR (武漢病毒) OR (沙斯-cov-2) OR (流感) OR (电 晕) OR (病毒) OR (科維德-19) OR (科维德-19) OR (細菌) OR (细菌) OR (電 暈) OR (독감) OR (바이러스) OR (박테리아) OR (사스 코브-2) OR (우한 바이러스) OR (인플루엔자) OR (중국 바이러스) OR (중국어 바이러스) OR (코로나) OR (코로나 바이러스) OR (코비드-19)';


CREATE MATERIALIZED VIEW search_corona_text_en AS
SELECT
    id_articles,
    lang,
    articles.hostname,
    pub_time,
    title,
    (   urls.scheme || '://' ||
        urls.hostname || path ||
        case when length(urls.params)=0 then '' else ';' || urls.params end ||
        case when length(urls.query)=0 then '' else '?' || urls.query end ||
        case when length(urls.fragment)=0 then '' else '#' || urls.fragment end
    ) as url
FROM articles 
INNER JOIN urls ON urls.id_urls = 
    (CASE WHEN articles.id_urls_canonical = 2425 
         THEN articles.id_urls 
         ELSE articles.id_urls_canonical END)
WHERE
    text &@~ '(bacteria) OR (virus) OR (corona) OR (coronavirus) OR (covid) OR (covid-19) OR (influenza) OR (sars-cov-2)';

