#! /usr/bin/python3

# command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',type=str,default='postgres:///novichenkobot')
#parser.add_argument('--query',type=str,required=True)
parser.add_argument('--output',type=str,required=True)
args = parser.parse_args()

# get query
# FIXME: should not be hardcoded

#args.query="select * from search_corona;"
args.query='''
    SELECT * 
    FROM (SELECT DISTINCT ON (hostname,lower(title)) * from search_corona)t
    where pub_time>'0001-01-01' or pub_time is null
    '''

# kim query
#args.query="select * from (select distinct on (title) * from search_kim where pub_time>='2020-04-18' and pub_time<='2020-04-25')t order by hostname,pub_time;"

# validate inputs
if '.jsonl.gz' in args.output:
    args.format = '.jsonl.gz'
elif '.xlsx' in args.output:
    args.format = '.xlsx'
else:
    raise ValueError('ERROR: output extension not recognized')

# database connection
import sqlalchemy
print('connect to db')
engine = sqlalchemy.create_engine(args.db, connect_args={
    'connect_timeout': 120,
    'application_name': 'NovichenkoAnalyze_query2xlsx',
    })
connection = engine.connect()

# get the data
print('running query')
sql=sqlalchemy.sql.text(args.query)
res = connection.execute(sql)

# generate output
print('generating output')
if args.format=='.jsonl.gz':
    import json
    import gzip
    with gzip.open(args.output,'xt') as f:
        for i,row in enumerate(res):
            try:
                f.write(json.dumps(dict(row),default=str)+'\n')
            except:
                print('i=',i,'error?')

if args.format=='.xlsx':
    import xlsxwriter
    rows = list(res)
    workbook = xlsxwriter.Workbook(args.output)
    worksheet = workbook.add_worksheet()

    for i,key in enumerate(rows[0].keys()):
        worksheet.write(0,i,key)

    for i,row in enumerate(rows):
        for j,key in enumerate(row.keys()):
            worksheet.write(i+1,j,row[j])

    workbook.close()
