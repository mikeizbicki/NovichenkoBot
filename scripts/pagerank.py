# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--alpha',type=float,default=0.1)
parser.add_argument('--beta',type=float,default=0.0)
parser.add_argument('--epsilon',type=float,default=1e-6)
parser.add_argument('--max_iters',type=int,default=10000)
parser.add_argument('--display_num',type=int,default=20)
parser.add_argument('--allow_self_link',action='store_true')
parser.add_argument('--cuda',type=int,default=None)
parser.add_argument('--type',choices=['all','keywords'],default='keywords')
parser.add_argument('--name',type=str,default=None)
parser.add_argument('--allow_bans',action='store_true')
parser.add_argument('--allow_selflinks',action='store_true')
parser.add_argument('--normed',action='store_true')
args = parser.parse_args()

# imports
import sqlalchemy
import os
import datetime

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_url_info,insert_request

# create database connection
engine = sqlalchemy.create_engine(args.db)
connection = engine.connect()

# load numerical libs
import torch

# load connection matrix from db
sql = sqlalchemy.sql.text('''
SELECT max(id_hostnames) FROM hostnames;
''');
res = connection.execute(sql)
num_hostnames = res.first()[0]+1
print('num_hostnames=',num_hostnames)

allow_selflinks=''
if not args.allow_selflinks:
    allow_selflinks='refs.hostname_source != refs.hostname_target AND'

allow_bans=''
if not args.allow_bans:
    allow_bans='''
    refs.hostname_source NOT IN (SELECT hostname FROM crawlable_hostnames WHERE priority='ban') AND
    refs.hostname_target NOT IN (SELECT hostname FROM crawlable_hostnames WHERE priority='ban') AND
    '''

sql = sqlalchemy.sql.text(f'''
SELECT 
    h1.id_hostnames as id_source,
    h2.id_hostnames as id_target,
    refs.distinct_all,
    refs.distinct_keywords,
    refs.distinct_all/(1e-20+totals.distinct_all) as distinct_all_normed,
    refs.distinct_keywords/(1e-20+totals.distinct_keywords) as distinct_keywords_normed
FROM refs_summary_simple as refs
LEFT JOIN (
    SELECT 
        hostname_source,
        sum(distinct_all) as distinct_all,
        sum(distinct_keywords) as distinct_keywords
    FROM refs_summary_simple
    GROUP BY hostname_source
) totals ON totals.hostname_source=refs.hostname_source
INNER JOIN hostnames h1 ON h1.hostname=refs.hostname_source 
INNER JOIN hostnames h2 ON h2.hostname=refs.hostname_target
WHERE
    {allow_selflinks}
    {allow_bans}
    TRUE
;
''')
res = connection.execute(sql)
connections = list(res)

print('constructing tensors')
indexes = torch.LongTensor([[id_source,id_target] for id_source,id_target,distinct_all,distinct_keywords,distinct_all_normed,distinct_keywords_normed in connections]).t()
values = torch.FloatTensor([[distinct_all,distinct_keywords,distinct_all_normed,distinct_keywords_normed] for id_source,id_target,distinct_all,distinct_keywords,distinct_all_normed,distinct_keywords_normed in connections]).t()

print('indexes.shape=',indexes.shape)
print('values.shape=',values.shape)

# initialize torch variables
connections_all = torch.sparse.FloatTensor(indexes,values[0,:],torch.Size([num_hostnames,num_hostnames]))
connections_keywords = torch.sparse.FloatTensor(indexes,values[1,:],torch.Size([num_hostnames,num_hostnames]))
connections_all_normed = torch.sparse.FloatTensor(indexes,values[2,:],torch.Size([num_hostnames,num_hostnames]))
connections_keywords_normed = torch.sparse.FloatTensor(indexes,values[3,:],torch.Size([num_hostnames,num_hostnames]))

if args.type=='keywords':
    if args.normed:
        A = connections_keywords_normed
    else:
        A = connections_keywords
elif args.type=='all':
    if args.normed:
        A = connections_all_normed
    else:
        A = connections_all

print('A.shape=',A.shape)

beta = args.beta
A = beta * A.t() + (1-beta)*A

alpha = args.alpha
v = torch.ones([num_hostnames])/num_hostnames
x = torch.ones([num_hostnames])/num_hostnames

v = torch.reshape(v,[num_hostnames,1])
x = torch.reshape(x,[num_hostnames,1])

if args.cuda is not None:
    device = torch.device(f'cuda:{args.cuda}')
    A = A.to(device)
    v = v.to(device)
    x = x.to(device)

# perform calculations
def iterate_pagerank():
    global x
    for i in range(0,args.max_iters):
        x_old = x
        x = (1-alpha)*torch.sparse.mm(A,x) + alpha*v
        x /= torch.norm(x,2)
        dist_inf = torch.dist(x_old,x,float('inf'))
        dist_2 = torch.dist(x_old,x,2)
        dist_1 = torch.dist(x_old,x,1)
        dist_0 = torch.dist(x_old,x,0)
        print(datetime.datetime.now(),f' i={i} dist_inf=%0.4g dist_2=%0.4g dist_1=%0.4g dist_0=%0.4g'%(dist_inf,dist_2,dist_1,dist_0))
        if dist_2 < args.epsilon:
            break
iterate_pagerank()

def print_topk():
    global x
    topk = torch.topk(x,args.display_num,dim=0)
    hostnames = tuple(torch.topk(x,args.display_num,dim=0).indices[:,0].tolist())
    sql = sqlalchemy.sql.text(f'''
    SELECT id_hostnames,hostname
    FROM hostnames
    WHERE id_hostnames in {hostnames}
    ;
    ''');
    res = connection.execute(sql)
    rows = list(res)

    hostname_size = max([len(row['hostname']) for row in rows])
    for row in rows:
        print(f'%20s %{hostname_size+1}s %0.4e'%(str(row['id_hostnames']),row['hostname'],x[row['id_hostnames'],0]))
print_topk()

# add results back into db
print('adding results into db')
if args.name is not None:
    with connection.begin() as trans:
        for i in range(num_hostnames):
            if i%100000==0:
                print(datetime.datetime.now(),f'i={i}')
            sql = sqlalchemy.sql.text('''
            INSERT INTO pagerank (id_hostnames,name,score)
            VALUES (:id_hostnames,:name,:score)
            ON CONFLICT(id_hostnames,name) DO UPDATE SET score=:score
            ''')
            res = connection.execute(sql,{'id_hostnames':i,'name':args.name,'score':x[i].item()})
