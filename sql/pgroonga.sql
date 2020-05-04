CREATE EXTENSION pgroonga;

CREATE INDEX articles_index_pgroonga_title ON articles USING pgroonga(title) TABLESPACE fastdata;
CREATE INDEX articles_index_pgroonga_text ON articles USING pgroonga(text);

--CREATE INDEX articles_index_pgroonga_title ON articles USING pgroonga(title)
--CREATE INDEX articles_index_pgroonga_title ON articles USING pgroonga(title)
--WITH (tokenizer='TokenNgram("unify_alphabet", false, "unify_digit", false, "unify_symbol", false)');
--CREATE INDEX articles_index_pgroonga_title ON articles USING pgroonga(title)
--WITH (tokenizer='TokenNgram("n",5,"unify_alphabet", false, "unify_digit", false, "unify_symbol", false)');

--CREATE INDEX articles_index_pgroonga_text5 ON articles USING pgroonga(text) WITH (tokenizer='TokenNgram("n",5)');
--CREATE INDEX articles_index_pgroonga_text2 ON articles USING pgroonga(left(text,4*1024*1024)); 



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

CREATE MATERIALIZED VIEW search_kim AS
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
    title &@~ '(ﻚﻴﻣ ﺝﻮﻨﻏ ﺃﻮﻧ) OR (یﻡ ﺝﻮﻧگ-ﺱﺍﺰﻣﺎﻧ) OR (金正恩) OR (김정은) OR (Ким Чен Ун) OR (Кім Чен-ООН) OR (کﻡ ﺝﻮﻧگ ﺎﻧ) OR (金正恩) OR (金正恩) OR (kim jong un) OR (kim jong-un) OR (kim jongun)';

select country,hostnames.lang,t.lang,t.hostname,count(*) 
from (select distinct on (hostname,title) * from search_kim)t 
left join hostnames on hostnames.hostname=t.hostname 
group by t.hostname,hostnames.country,hostnames.lang,t.lang
order by t.hostname,count(*) desc;


