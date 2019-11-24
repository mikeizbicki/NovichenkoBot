#! /usr/bin/python3

# command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',type=str,default='postgres:///novichenkobot')
parser.add_argument('--max_doc_size',type=int,default=999999999)
parser.add_argument('--max_docs',type=int,default=999999999)
parser.add_argument('--claim',type=str,required=True)
parser.add_argument('--seed',type=int,default=0)
parser.add_argument('--labelers',type=str,nargs='+',required=True)
args = parser.parse_args()

# import libraries
import xlsxwriter
import sqlalchemy
import random

# database connection
print('connect to db')
engine = sqlalchemy.create_engine(args.db, connect_args={'connect_timeout': 120})
connection = engine.connect()

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_id_articles,get_sentences
from NovichenkoBot.spiders.general_spider import GeneralSpider

# define column types
claim_attrs=[]
claim_attrs.append({
    'title':'DocType',
    'validator':{
        'validate' : 'list',
        'source' : ['url','tweet']
        },
    'width':8,
    'default':'',
    'format':{'locked':1},
    })
claim_attrs.append({
    'title':'DocID',
    'validator':None,
    'width':8,
    'default':'',
    'format':{'locked':1},
    })
claim_attrs.append({
    'title':'Category',
    'validator':{
        'validate' : 'list',
        'source' : ['title','subtitle','body'],
        },
    'width':8,
    'default':'',
    'format':{'locked':1},

    })
claim_attrs.append({
    'title':'Sentence',
    'validator': None,
    'width':60,
    'default':'',
    'format':{'locked':0},

    })
#claim_attrs.append({
    #'title':'Type',
    #'validator':{
        #'validate' : 'list',
        #'source' : [
            #'1. personal experience',
            #'2. quantity in the past or present',
            #'3. correlation or causation',
            #'4. current laws or rules of operation',
            #'5. prediction',
            #'6. other type of claim',
            #'7. not a claim',
            #],
        #},
    #'width':20,
    #'default':'',
    #'format':{'locked':0},
    #})
for title in ['Events','Regulations','Quantity','Prediction','Personal','Normative','Other','No Claim']:
    claim_attrs.append({
        'title':title,
        'validator':{
            'validate' : 'list',
            'source' : [
                '',
                'X',
                ],
            },
        'width':10,
        'default':'',
        'special':True,
        'format':{'locked':0, 'left':1, 'right':1, 'border_color':'#dddddd' },
        })
claim_attrs.append({
    'title':'Citation',
    'validator':{
        'validate' : 'list',
        'source' : [
            '1. 3rd person source (named)',
            '2. 3rd person source (anonymous)',
            '3. 1st person source',
            '4. news source',
            '5. other source',
            '6. no citation',
            ],
        },
    'width':20,
    'default':'6. no citation',
    'format':{'locked':0},
    })
claim_attrs.append({
    'title':'Claim Stance',
    'validator':{
        'validate' : 'list',
        'source' : [
            '1. support',
            '2. refute',
            '3. discuss',
            '4. unrelated'
            ],
        },
    'width':20,
    'default':'4. unrelated',
    'format':{'locked':0},
    })
claim_attrs.append({
    'title':'Bias',
    'validator':{
        'validate' : 'list',
        'source' : [
            '1. in favor of north korea',
            '2. neutral',
            '3. against north korea',
            ],
        },
    'width':20,
    'default':'2. neutral',
    'format':{'locked':0},
    })
#claim_attrs.append({
    #'title':'Comments',
    ##'validator':{
        ##'validate' : 'list',
        ##'source' : [
            ##'1. no',
            ##'2. yes',
            ##],
        ##},
    ##'default':'1. no',
    #'validator':None,
    #'default':'',
    #'width':20,
    #'format':{'locked':0},
    #})
#claim_attrs.append({
    #'title':'Doc Stance',
    #'validator':{
        #'validate' : 'list',
        #'source' : [
            #'1. support',
            #'2. refute',
            #'3. discuss',
            #'4. unrelated'
            #],
        #},
    #'width':20,
    #'default':'4. unrelated',
    #'format':{'locked':0},
    #})
#claim_attrs.append({
    #'title':'Important',
    #'validator':{
        #'validate' : 'list',
        #'source' : ['high','low'],
        #},
    #'width':20,
    #'default':'low'
    #})

class MySheet:
    def __init__(self,filename,columns,title):
        self.workbook = xlsxwriter.Workbook(filename)
        self.columns=columns
        self.doc_row_start=0
        self.worksheet = self.workbook.add_worksheet()
        self.current_row=2
        self.current_doc=0

        center = self.workbook.add_format({'align':'center'})
        center_bold = self.workbook.add_format({'align':'center','bold': True})

        self.worksheet.merge_range(0,0,0,len(self.columns),title,center_bold)
        for i in range(len(self.columns)):
            self.worksheet.set_column(i,i,self.columns[i]['width'])
            if 'special' not in columns[i]:
                self.worksheet.merge_range(1,i,2,i,columns[i]['title'],center)
            else:
                self.worksheet.write(2,i,self.columns[i]['title'],center)
        self.worksheet.merge_range(1,4,1,11,'Claim Type',center)
        self.worksheet.freeze_panes(3,0)
        self.worksheet.protect()

    def close(self):
        self.workbook.close()

    def add_validation_rows(self):
        max_options=20 #FIXME
        self.current_row+=10
        for opt in range(max_options):
            self.current_row+=1
            self.worksheet.write(self.current_row,0,'discard')
            for i in range(1,len(self.columns)):
                try:
                    self.worksheet.write(self.current_row,i,self.columns[i]['validator']['source'][opt])
                except:
                    pass

    def get_format(self,col_id,locked_overwrite=False):
        if self.current_doc%2==0:
            if (self.doc_row_start-self.current_row)%2==0:
                color='#ffeeee'
            else:
                color='#ffdddd'
        else:
            if (self.doc_row_start-self.current_row)%2==0:
                color='#eeeeff'
            else:
                color='#ddddff'
        format_dict={
            'text_wrap': 1, 
            'valign': 'top', 
            'bg_color': color,
            }
        if 'format' in self.columns[col_id].keys():
            format_dict.update(self.columns[col_id]['format'])
        if locked_overwrite:
            #format_dict['format']={'locked':1} #['locked']=1
            #format_dict['validator']=None
            format_dict.update({'locked':1})
        return self.workbook.add_format(format_dict)

    def add_default_row(self):
        self.current_row+=1
        self.worksheet.set_row(self.current_row,45)
        for i in range(len(self.columns)):
            self.worksheet.write(self.current_row,i,self.columns[i]['default'],self.get_format(i))
            if self.columns[i]['validator']:
                self.worksheet.data_validation(self.current_row,i,self.current_row,i,self.columns[i]['validator'])

    def begin_document(self):
        self.current_doc+=1
        self.doc_row_start=self.current_row+1

    def merge_document_columns(self,col_name,val):
        col=None
        for i in range(len(self.columns)):
            if self.columns[i]['title']==col_name:
                col=i
        self.worksheet.merge_range(self.doc_row_start,col,self.current_row,col,val,self.get_format(col))

    def update_column(self,col_name,val,locked_overwrite=False):
        col=None
        for i in range(len(self.columns)):
            if self.columns[i]['title']==col_name:
                col=i
        if len(val)>3*self.columns[col]['width']:
            height=15*(1+len(val)/self.columns[col]['width'])
            self.worksheet.set_row(self.current_row,height)
        self.worksheet.write(self.current_row,col,val,self.get_format(col,locked_overwrite=locked_overwrite))

########################################
print('downloading URLs')
import newspaper 
with open(args.claim+'.claim') as f:
    #title=''.join(f.readlines())
    claim_title=f.readlines()[0].strip()
    print('claim_title=',claim_title)

with open(args.claim+'.urls') as f:
    urls=[]
    for url in f.readlines()[:args.max_docs]:
        url=url.strip()
        urls.append(url)
    id_articles_list=get_id_articles(connection,urls)

print('len(id_articles_list)=',len(id_articles_list))
print('id_articles_list=',id_articles_list)

########################################
print('creating xlsx files')
for labeler in args.labelers:
    filename=args.claim+'.'+labeler+'.xlsx'
    claim_sheet=MySheet(filename,claim_attrs,claim_title)
    random.Random(args.seed+hash(labeler)).shuffle(id_articles_list)
    for id_articles in id_articles_list:
        sentences=get_sentences(connection,id_articles)
        claim_sheet.begin_document()
        for i,sentence in enumerate(sentences[:args.max_doc_size]):
            claim_sheet.add_default_row()
            if i==0:
                claim_sheet.update_column('Category','title')
            else:
                claim_sheet.update_column('Category','body')
            claim_sheet.update_column('Sentence',sentence)
        claim_sheet.add_default_row()
        claim_sheet.update_column('Category','overall')
        claim_sheet.update_column('Sentence','<<<< rate the article overall >>>>')
        for header in ['Events','Regulations','Quantity','Prediction','Personal','Normative','Other','No Claim', 'Citation']:
            claim_sheet.update_column(header,'N/A',locked_overwrite=True)
        claim_sheet.merge_document_columns('DocID',id_articles) # FIXME: url?
        claim_sheet.merge_document_columns('DocType','url')
    claim_sheet.add_validation_rows()
    claim_sheet.close()
