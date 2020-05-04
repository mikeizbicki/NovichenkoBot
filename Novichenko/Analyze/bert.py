# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--id_articles0',type=int)
parser.add_argument('--articles_per_iteration',type=int,default=512)
args = parser.parse_args()

# imports
import sqlalchemy
import os
import datetime
import sys

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from Novichenko.Bot.sqlalchemy_utils import get_url_info,insert_request

# create database connection
engine = sqlalchemy.create_engine(args.db, connect_args={
    'application_name': 'Novichenko.Analyze.Bert',
    })
connection = engine.connect()

# disable warnings
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import tensorflow as tf

# make pytorch deterministic
import torch
torch.manual_seed(0)
torch.backends.cudnn.deterministic = True
torch.backends.cudnn.benchmark = False
import numpy as np
np.random.seed(0)

# generate random projection matrix
# if pytorch is not deterministic,
# then this projection matrix will change between program runs,
# invalidating the nearest neighbor results
embedding_size = 100
A = torch.randn([768,embedding_size]).cuda()
u, s, v = torch.svd(A)
projection = u

# prepare transformers library
import transformers
model_name = 'bert-base-multilingual-uncased'
tokenizer = transformers.BertTokenizer.from_pretrained(model_name)
bert = transformers.BertModel.from_pretrained(model_name).cuda()

# the main worker loop
import itertools
for iteration in itertools.count(0):
    print(datetime.datetime.now(),'articles=',iteration*args.articles_per_iteration)

    # get the next unlabeled article raw data from the db
    sql=sqlalchemy.sql.text('''
    SELECT articles.id_articles,title 
    FROM articles
    LEFT JOIN articles_title_bert ON articles.id_articles = articles_title_bert.id_articles
    WHERE articles_title_bert.id_articles is null
      AND articles.pub_time is not null
      AND articles.id_articles > :id_articles0
    LIMIT :limit
    ''')
    res = connection.execute(sql,{
        'offset':0,
        'id_articles0':args.id_articles0,
        'limit':args.articles_per_iteration
        })
    rows = [ dict(row) for row in res ]

    # exit if no rows remaining
    if len(rows)==0:
        sys.exit(0)

    # generate the encoding tensors tha will be input into bert
    maxlen = max([ len(row['title']) for row in rows ])
    for row in rows:
        row['encoding'] = tokenizer.encode_plus(
            row['title'],
            add_special_tokens = True,
            max_length = maxlen,
            pad_to_max_length = True,
            return_attention_mask = True,
            return_tensors = 'pt',
            )
        row['encoding']['input_ids'].cuda()
        row['encoding']['attention_mask'].cuda()
    input_ids = torch.cat([ row['encoding']['input_ids'] for row in rows ],dim=0).cuda()
    attention_mask = torch.cat([ row['encoding']['attention_mask'] for row in rows ],dim=0).cuda()

    # apply bert to generate the embedding
    with torch.no_grad():
        res = bert(input_ids, attention_mask)
        last_layer,embedding = res
        embedding = torch.mean(last_layer,dim=1)
        embedding = torch.einsum('ab,bc->ac',embedding,projection) 

    # store the embeddings in the db
    with connection.begin() as trans:
        for i,row in enumerate(rows):
            sql=sqlalchemy.sql.text('''
            INSERT INTO articles_title_bert (id_articles,embedding) 
            VALUES (:id_articles,cube(:embedding))
            ON CONFLICT DO NOTHING;
            ''')
            res = connection.execute(sql,{
                'id_articles':row['id_articles'],
                'embedding':list(embedding[i,:].tolist())
                })
