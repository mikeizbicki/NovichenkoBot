set -e

########################################
# settings
########################################

db=novichenkobot
db_rfc=postgres:///$db
num_jobs=100

########################################
# create log directories
########################################

log=log/$(date +'%Y-%m-%d-%H-%M-%S')
mkdir -p $log
ln -sfn "$(pwd)/$log" log/newest

########################################
# reset postgres stats monitors
########################################

psql -c 'select pg_stat_reset ();' > /dev/null
psql -c 'select pg_stat_statements_reset ();' > /dev/null

########################################
# launch the high priority crawls
########################################

res=$(psql $db -c "select hostname from crawlable_hostnames where priority='high' order by hostname;")
hostnames_high=$(echo "$res" | tail -n +4 | head -n -3)

for hostname in $hostnames_high; do
    echo "high priority crawl: $hostname"
    nice -n -10 scrapy crawl general -s HOSTNAME_RESTRICTIONS=$hostname -a db=$db_rfc > $log/general-$hostname 2>&1 &
    echo $! >> $log/pids
done

########################################
# launch the generic crawler
########################################

#for i in $(seq 0 0); do
    #echo "generic crawl: offset index = $i"
    #nohup nice -n -19 scrapy crawl general -s OFFSET_INDEX=$i -s ROBOTSTXT_OBEY=False -s MEMQUEUE_LIMIT=50000 -s DNS_TIMEOUT=15 -s DOWNLOAD_TIMEOUT=15 -s CONCURRENT_REQUESTS=128 -s REACTOR_THREADPOOL_MAXSIZE=64 -s AUTOTHROTTLE_ENABLED=False -a db=$db_rfc > $log/allhosts-$(printf "%04d" $i) 2>&1 &
    #echo $! >> $log/pids
#done

########################################
# launch the low priority crawls
########################################

# NOTE: started focusing on new languages when max(id_frontier)=618645536

method='tld3'

if [ $method = tld ]; then
    #res=$(psql $db -c "
    #select hostname_target
    #from refs_summary_simple 
    #left join hostname_productivity on hostname_productivity.hostname = refs_summary_simple.hostname_target
    #where hostname_target like '%.kr' and hostname is null
    #group by hostname_target
    #order by sum(distinct_keywords) desc
    #limit 500
    #")
    res=$(psql $db -c "
    SELECT hostname
    --SELECT hostname,num_1000000,num_100000,num_10000,num_1000,num_100,num_10,num_0
    FROM frontier_hostname
    WHERE 
        --hostname NOT IN (SELECT hostname FROM responses_timestamp_hostname_hostnames) 
        hostname NOT IN (SELECT hostname FROM requests_hostname) 
        --and (hostname like '%.kr' or hostname like '%.jp' or hostname like '%.cn' or hostname like '%.ru')
        --and (right(hostname,3) in ('.kr','.jp','.cn','.ru'))
        and (right(hostname,3) in ('.kr','.jp','.cn','.ru','.ag','.ar','.bb','.bo','.br','.bs','.bz','.ci','.cl','.co','.cr','.do','.ec','.es','.fk','.fj','.gd','.gf','.gp','.gq','.gt','.gy','.hn','.ht','.jm','.kn','.mq','.nc','.ni','.pa','.pr','.pt','.py','.sr','.st','.sv','.tt','.uy','.vc','.ve'))
    ORDER BY num_1000000 desc,num_100000 desc,num_10000 desc,num_1000 desc,num_100 desc,num_10 desc,num_0 desc
    limit 500;
    ")
elif [ $method = tld2 ]; then
    res=$(psql $db -c "
    select hostname from hostname_productivity where hostname like '%.kr' or hostname like '%.jp';
    ")
elif [ $method = tld3 ]; then
    res=$(psql $db -c "
    SELECT DISTINCT hostname
    FROM (
        SELECT hostname_productivity.hostname,valid_fraction
        FROM hostname_productivity
        INNER JOIN hostname_progress ON hostname_progress.hostname = hostname_productivity.hostname
        WHERE 
            fraction_requested < 0.5
            and hostname_productivity.hostname not in 
                ( SELECT hostname FROM crawlable_hostnames WHERE priority in ('ban','high'))
            -- valid_fraction > 0.5 and
            -- right(hostname,3) in ('.ru','.br','.pt')
            -- (right(hostname,3) in ('.kr','.jp','.cn','.ru','.ag','.ar','.bb','.bo','.br','.bs','.bz','.ci','.cl','.co','.cr','.do','.ec','.es','.fk','.fj','.gd','.gf','.gp','.gq','.gt','.gy','.hn','.ht','.jm','.kn','.mq','.nc','.ni','.pa','.pr','.pt','.py','.sr','.st','.sv','.tt','.uy','.vc','.ve'))
        order by score*valid_fraction desc
        limit $(($num_jobs * 3))
        )t;
    ")
elif [ $method = lang ]; then
    res=$(psql $db -c "
    select hostname 
    from crawlable_hostnames 
    where 
        priority='' and 
        (lang='zh' or lang='ko' or lang='ja')
    order by hostname;
    ")
elif [ $method = hostname_productivity ]; then
    res=$(psql $db -c "
    SELECT hostname
    FROM hostname_productivity
    ORDER BY priority desc
    limit 500;
    ")
elif [ $method = frontier_priority ]; then
    res=$(psql $db -c "
    SELECT DISTINCT hostname
    FROM (
        SELECT substring(reverse(hostname_reversed) from 2) as hostname,priority
        FROM frontier
        WHERE 
            timestamp_processed is null
            and substring(reverse(hostname_reversed) from 2) not in (
                SELECT hostname FROM crawlable_hostnames WHERE priority in ('ban','high')
            )
        ORDER BY priority DESC
        LIMIT 100000
    )t
    LIMIT 500
    ;
    ")
elif [ $method = frontier_priority2 ]; then
    res=$(psql $db -c "
    SELECT DISTINCT hostname
    FROM (
        SELECT substring(reverse(hostname_reversed) from 2) as hostname,frontier.priority
        FROM frontier
        INNER JOIN hostname_productivity on hostname_productivity.hostname =  substring(reverse(frontier.hostname_reversed) from 2)
        WHERE 
            hostname_productivity.valid_fraction > 0.5 AND
            timestamp_processed is null
            and substring(reverse(hostname_reversed) from 2) not in (
                SELECT hostname FROM crawlable_hostnames WHERE priority in ('ban','high')
            )
            --and (right(hostname,3) in ('mil','gov','org','edu','.iq','.ir','.kr','.jp','.cn','.ru','.ag','.ar','.bb','.bo','.br','.bs','.bz','.ci','.cl','.co','.cr','.do','.ec','.es','.fk','.fj','.gd','.gf','.gp','.gq','.gt','.gy','.hn','.ht','.jm','.kn','.mq','.nc','.ni','.pa','.pr','.pt','.py','.sr','.st','.sv','.tt','.uy','.vc','.ve'))
        ORDER BY frontier.priority DESC
        LIMIT 100000
    )t
    LIMIT 500
    ;
    ")
elif [ $method = frontier_hostname ]; then
    res=$(psql $db -c "
    SELECT hostname 
    FROM frontier_hostname 
    WHERE num_0>0 AND hostname NOT IN (SELECT hostname FROM responses_timestamp_hostname_hostnames) 
    ORDER BY num_1000000,num_100000,num_10000,num_1000,num_100,num_10,num_0
    LIMIT 500;
    ")
elif [ $method = crawlable_hostnames ]; then
    res=$(psql $db -c "
    select hostname_target from (
    select hostname_target,sum(num) as num
    from refs_keywords
    where
        type='link' and
        hostname_source in (select hostname from hostname_productivity limit 5000) and
        hostname_target not in (select hostname from crawlable_hostnames) and
        right(hostname_target, length(hostname_target)-4) not in (SELECT hostname FROM crawlable_hostnames) and
        hostname_target not in (select distinct hostname from responses_timestamp_hostname)
    group by hostname_target
    order by num desc
    ) as t1
    limit $(( 5 * $num_jobs ));
    ")
else
    echo 'no method for low priority crawls specified'
    exit
fi

hostnames_low=$(echo "$res" | tail -n +4 | head -n -3)

# log the hostnames 
i=0
for hostname in $hostnames_low; do
    echo ${hostname} >> $log/hostnames.$(printf "%04d" $(( i % $num_jobs )) )
    i=$(( $i + 1 ))
done

# launch low priority crawlers
i=0
for file in $log/hostnames.*; do
    restrictions=$(cat $log/hostnames.$(printf "%04d" $i) | xargs echo | tr ' ' ',')
    echo $i : $restrictions
    nohup nice -n 19 scrapy crawl general -s HOSTNAME_RESTRICTIONS=$restrictions -a db=$db_rfc > $log/general.$(printf "%04d" $i) 2>&1 &
    echo $! >> $log/pids
    i=$(( i + 1 ))
done
