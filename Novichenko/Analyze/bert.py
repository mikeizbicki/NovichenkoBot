# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',default='postgres:///novichenkobot')
parser.add_argument('--source',required=True)
parser.add_argument('--id0',type=int, default=0)
parser.add_argument('--texts_per_iteration',type=int,default=256)
parser.add_argument('--max_length',type=int,default=None)
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

# get id_source;
# we do this before the other initialization code below because that code can take a while to run,
# and the checks here can give us more immediate feedback on whether the input parameters are acceptable
table_schema = args.source.split('.')[0]
table_name = args.source.split('.')[1]
column_name = args.source.split('.')[2]
sql=sqlalchemy.sql.text('''
    SELECT id_sources
    FROM embeddings.sources
    WHERE table_schema=:table_schema 
      AND table_name=:table_name 
      AND column_name=:column_name
    ''')
res = connection.execute(sql,{
    'table_schema':table_schema,
    'table_name':table_name,
    'column_name':column_name,
    })
try:
    id_sources = res.first()['id_sources']
except TypeError:
    raise ValueError(f'--source={args.source} not contained in embeddings.sources table')

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
    if iteration%100==0 or iteration<100:
        print(datetime.datetime.now(),'texts=',iteration*args.texts_per_iteration)

    # get the next unlabeled text raw data from the db
    sql=sqlalchemy.sql.text(f'''
    SELECT {table_name}.id_{table_name},{column_name} 
    FROM {table_schema}.{table_name}
    LEFT JOIN embeddings.bert ON {table_name}.id_{table_name} = embeddings.bert.id AND embeddings.bert.id_sources = {id_sources}
    WHERE embeddings.bert.id is null
      AND {table_name}.id_{table_name} > :id0
    LIMIT :limit
    ''')
    res = connection.execute(sql,{
        'offset':0,
        'id0':args.id0,
        'limit':args.texts_per_iteration
        })
    rows = [ dict(row) for row in res ]

    ids = [ row['id_'+table_name] for row in rows ]
    texts = [ row[column_name] for row in rows ]

    # exit if no rows remaining
    if len(rows)==0:
        sys.exit(0)

    # apply bert to generate the embeddings
    with torch.no_grad():

        # first generate the encoding in two attempts;
        # the first attempt uses an arbitrary length max_length value,
        # which will make BERT much faster on short texts;
        # if this results in an encoding that is too long, however,
        # then we must resort to specifying the max_length
        encodings = tokenizer.batch_encode_plus(
            texts,
            max_length = args.max_length,
            pad_to_max_length = True,
            return_tensors = 'pt'
            )
        if encodings['input_ids'].shape[1]>512:
            encodings = tokenizer.batch_encode_plus(
                texts,
                max_length = 512,
                pad_to_max_length = True,
                return_tensors = 'pt'
                )

        # convert the encoding into an embedding with BERT
        res = bert(encodings['input_ids'].cuda())
        last_layer,_ = res
        embedding = torch.mean(last_layer,dim=1)
        embedding = torch.einsum('ab,bc->ac',embedding,projection) 

    # store the embeddings in the db
    with connection.begin() as trans:
        for i,id in enumerate(ids):
            sql=sqlalchemy.sql.text('''
            INSERT INTO embeddings.bert (id_sources,id,embedding) 
            VALUES (:id_sources,:id,cube(:embedding))
            ON CONFLICT DO NOTHING;
            ''')
            res = connection.execute(sql,{
                'id':id,
                'id_sources':id_sources,
                'embedding':list(embedding[i,:].tolist())
                })
