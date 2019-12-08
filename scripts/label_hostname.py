#! /usr/bin/python3

# command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',type=str,default='postgres:///novichenkobot')
parser.add_argument('--print_summary',action='store_true')
parser.add_argument('--hostname',type=str,required=True)
parser.add_argument('--articles_per_iteration',type=int,default=100)
parser.add_argument('--debug_mode',action='store_true')
parser.add_argument('--CUDA_VISIBLE_DEVICES',default='0')
parser.add_argument('--rotate_gpu',action='store_true')
args = parser.parse_args()

import os
os.environ['CUDA_VISIBLE_DEVICES']=args.CUDA_VISIBLE_DEVICES

# import libraries
import datetime
import numpy as np
import sqlalchemy
from textblob import TextBlob
import subprocess
import sys
import time

# database connection
print('connect to db')
engine = sqlalchemy.create_engine(args.db, connect_args={'connect_timeout': 120})
connection = engine.connect()

########################################
# FIXME: should these label functions find a better home?

def polarity(text):
    return TextBlob(text).sentiment.polarity

def subjectivity(text):
    return TextBlob(text).sentiment.subjectivity

import textstat
label_funcs=[
    textstat.syllable_count,
    textstat.lexicon_count,
    textstat.flesch_reading_ease,
    textstat.smog_index,
    textstat.flesch_kincaid_grade,
    textstat.coleman_liau_index,
    textstat.automated_readability_index,
    textstat.dale_chall_readability_score,
    textstat.difficult_words,
    textstat.linsear_write_formula,
    textstat.gunning_fog,
    polarity,
    subjectivity
    ]


def tokenize_by_sentence(text):
    '''
    This function takes an input text and returns a list of sentences.
    It is intended as a helper function for `get_sentences` and not for direct use.
    '''

    # split into sentences
    import nltk
    paragraphs=text.split('\n\n')
    sentences_raw = []
    for paragraph in paragraphs:
        sentences_raw += nltk.tokenize.sent_tokenize(paragraph)
    
    # remove extra whitespace and empty sentences
    sentences = []
    for sentence_raw in sentences_raw:
        sentence=sentence_raw.strip()
        if sentence!='':
            sentences.append(sentence)

    return sentences


def get_sentences(connection,id_articles,text=None,title=None):
    '''
    Returns the list of sentences associated with an article.
    If the sentences have not already been added into the `sentences` table,
    then they are added.
    '''

    # if the sentences have already been extracted and inserted into the table,
    # then simply select those sentences and return
    sql=sqlalchemy.sql.text('''
        SELECT id_sentences,sentence
        FROM sentences
        WHERE id_articles=:id_articles
        ''')
    res=connection.execute(sql,{
        'id_articles':id_articles
        })
    sentences_with_ids=list(res)
    if sentences_with_ids != []:
        return [ text for id_sentences,text in sentences_with_ids ]

    # get the article text
    if title is None or text is None:
        sql=sqlalchemy.sql.text('''
            SELECT text,title 
            FROM articles 
            WHERE id_articles=:id_articles;
        ''')
        res=connection.execute(sql,{
            'id_articles':id_articles
            })
        text,title=res.first()

    # compute the sentences 
    sentences = [title]+tokenize_by_sentence(text)

    # if spacy/neuralcoref nlp toolbox not loaded, then load it
    global nlp
    try:
        nlp
    except NameError:
        import nltk
        import spacy
        nlp = spacy.load('en_core_web_lg')
        import neuralcoref
        neuralcoref.add_to_pipe(nlp)

    # compute the coref resolved sentences;
    # this process substitutes all pronouns with the referred to noun;
    text_resolved=nlp(text)._.coref_resolved
    sentences_resolved = [title]+tokenize_by_sentence(text_resolved)
    
    # there should be a 1-1 correspondence between the resolved and original sentences;
    # sometimes, the tokenizer breaks the resolved text into too many sentences,
    # and in that case we do not store any resolved text to indicate that a failure occurred
    if len(sentences) != len(sentences_resolved):
        sentences_resolved = [ None for sentence in sentences ]
    assert(len(sentences)==len(sentences_resolved))

    # insert sentences into table;
    # all insertions are in a single transaction to ensure that if any sentence 
    # of the article is included in the table `sentences`, then they all are
    with connection.begin() as trans:
        for i in range(len(sentences)):
            sql=sqlalchemy.sql.text('''
                INSERT INTO sentences
                    (id_articles,id_sentences,sentence,sentence_resolved)
                    VALUES
                    (:id_articles,:id_sentences,:sentence,:sentence_resolved)
            ''')
            connection.execute(sql,{
                'id_articles':id_articles,
                'id_sentences':i,
                'sentence':sentences[i],
                'sentence_resolved':sentences_resolved[i]
                })

    return sentences


def get_label(connection,id_articles,id_sentences,label_func,sentence=None):
    '''
    Returns the label of the sentence calculated according to `label_func`.
    If the label has previously been calculated and is stored in the `labels` table,
    then this function merely looks up the value and returns it.
    Otherwise, this function calculates the value and stores it in the table.
    '''

    # id_labels is a concatenation of the module and variable name
    # used in the original definition of label_func
    #id_labels=label_func.__module__+':'+label_func.__name__
    id_labels=label_func.__name__

    # if the label has already been calculated, 
    # then simply return the cached label
    sql=sqlalchemy.sql.text('''
        SELECT score
        FROM labels
        WHERE 
            id_articles=:id_articles AND 
            id_sentences=:id_sentences AND
            id_labels=:id_labels;
    ''')
    res=connection.execute(sql,{
        'id_articles':id_articles,
        'id_sentences':id_sentences,
        'id_labels':id_labels
        })
    val=res.first()
    if val is not None:
        return val[0]

    # if sentence not passed in, then we'll look it up in the database
    if sentence is None:
        sql=sqlalchemy.sql.text('''
            SELECT sentence
            FROM sentences
            WHERE 
                id_articles=:id_articles AND 
                id_sentences=:id_sentences;
        ''')
        res=connection.execute(sql,{
            'id_articles':id_articles,
            'id_sentences':id_sentences,
            })
        sentence=res.first()[0]

    # calculate the label
    score=label_func(sentence)

    # insert the label
    sql=sqlalchemy.sql.text('''
        INSERT INTO labels
        (id_articles,id_sentences,id_labels,score)
        VALUES
        (:id_articles,:id_sentences,:id_labels,:score);
    ''')
    res=connection.execute(sql,{
        'id_articles':id_articles,
        'id_sentences':id_sentences,
        'id_labels':id_labels,
        'score':score,
        })

    return score

def label_hostname(connection,hostname):

    # print summary stats of articles from hostname
    if args.print_summary:
        sql=sqlalchemy.sql.text('''
            SELECT count(1)
            FROM articles
            WHERE
                hostname=:hostname;
        ''')
        total_articles=connection.execute(sql,{
            'hostname':hostname
            }).first()[0]
        print(f'total_articles = {total_articles}')

        sql=sqlalchemy.sql.text('''
            SELECT count(1)
            FROM get_valid_articles(:hostname);
        ''')
        total_unique=connection.execute(sql,{
            'hostname':hostname
            }).first()[0]
        print(f'total_unique = {total_unique}')
        print(f'total_unique / total_articles = {total_unique / float(total_articles)}')

        sql=sqlalchemy.sql.text('''
            SELECT count(1)
            FROM get_valid_articles(:hostname)
            WHERE id_articles NOT IN (SELECT id_articles FROM sentences);
        ''')
        total_unprocessed=connection.execute(sql,{
            'hostname':hostname
            }).first()[0]
        print(f'total_unprocessed = {total_unprocessed}')
        print(f'total_unprocessed / total_unique = {total_unprocessed / float(total_unique)}')

    # loop through each unique article to label it
    i=-1
    while True:
        i+=1
        print(datetime.datetime.now(),f': i={i}')

        # get next article from hostname
        if not args.debug_mode:
            new_articles_condition = 'WHERE id_articles NOT IN (SELECT DISTINCT id_articles FROM sentences)'
        else:
            new_articles_condition = ''

        sql=sqlalchemy.sql.text(f'''
            SELECT id_articles,title,text,lang,id_urls_canonical_
            FROM get_valid_articles(:hostname)
            {new_articles_condition}
            LIMIT :limit;
        ''')
        res=connection.execute(sql,{
            'hostname':hostname,
            'limit':args.articles_per_iteration
            })
        rows=list(res)

        # otherwise, create the labels
        with connection.begin() as trans:

            # add python generated labels
            for row in rows:
                id_articles,title,text,lang,id_urls_canonical = row
                sentences = get_sentences(connection,id_articles,text=text,title=title)
                for id_sentences,sentence in enumerate(sentences):
                    for label_func in label_funcs:
                        get_label(connection,id_articles,id_sentences,label_func,sentence=sentence)

            # add labels from NVIDIA's Putchnik unsupervised sentiment classification library
            # this requires calling a command line utility
            # to use this utility, we first load the sentences into a temporary csv file
    
            # create a temporary file
            import tempfile
            tempfile_path = tempfile.mktemp()+'.csv'
            print(f'  {tempfile_path}')

            # load the csv file
            import csv
            with open(tempfile_path,'w',buffering=1) as f_tmp:
                csvwriter = csv.writer(f_tmp,quoting=csv.QUOTE_NONNUMERIC)
                csvwriter.writerow(['id_articles','id_sentences','sentence'])
                X_list=[]
                for row in rows:
                    id_articles,title,text,lang,id_urls_canonical = row
                    sentences = get_sentences(connection,id_articles,text=text,title=title)
                    for id_sentences,sentence in enumerate(sentences):
                        X_list.append([id_articles,id_sentences])
                        if sentence=='':
                            sentence=' '
                        csvwriter.writerow([id_articles,id_sentences,sentence])

            # the next steps are identical for both the mlstm and transformer models
            emotions=['anger','anticipation','disgust','fear','joy','sadness','surprise','trust']
            for modelname in ['mlstm','transformer']:
                batch_size=64

                while True:

                    # make the call and extract the labels from the resulting file
                    cmd=f'python3 libs/sentiment-discovery/run_classifier.py --load libs/sentiment-discovery/{modelname}_semeval.clf --text-key=sentence --data={tempfile_path} --save_probs={tempfile_path} --batch-size={batch_size} > {tempfile_path}_stdout 2> {tempfile_path}_stderr'
                    if modelname=='transformer':
                        cmd+=' --model=transformer'
                    ret = os.system(cmd)
                    if ret == 0:
                        break

                    # if the call failed, it is likely due to being out of memory on the current GPU
                    # by changing CUDA_VISIBLE_DEVICES, we select a new gpu 
                    print('os.system(cmd) failed')
                    print('CUDA_VISIBLE_DEVICES=',os.environ['CUDA_VISIBLE_DEVICES'])
                    print(f'batch_size={batch_size}')
                    batch_size //= 2

                    for std in ['stderr','stdout']:
                        print(80*'=')
                        print(std)
                        print(80*'=')
                        with open(f'{tempfile_path}_{std}') as f:
                            print(f.read())

                    if args.rotate_gpu:
                        os.environ['CUDA_VISIBLE_DEVICES']=str((int(os.environ['CUDA_VISIBLE_DEVICES'])+1)%8)
                        time.sleep(10)
                    elif batch_size==1:
                        sys.exit(1)

                labels=np.load(f'{tempfile_path}.prob.npy')
                print('labels.shape=',labels.shape)
                print('len(X_list)=',len(X_list))

                # load the labels into the database
                for X_list_index,[id_articles,id_sentences] in enumerate(X_list):
                    for emotions_index in range(len(emotions)):
                        sql=sqlalchemy.sql.text('''
                            INSERT INTO labels
                            (id_articles,id_sentences,id_labels,score)
                            VALUES
                            (:id_articles,:id_sentences,:id_labels,:score)
                            ON CONFLICT
                            DO NOTHING;
                        ''')
                        res=connection.execute(sql,{
                            'id_articles':id_articles,
                            'id_sentences':id_sentences,
                            'id_labels':modelname+'_'+emotions[emotions_index],
                            'score':float(labels[X_list_index,emotions_index]),
                            })

        # exit the loop if we have fully labeled the hostname or are in debug_mode
        if rows == [] or args.debug_mode:
            print('done')
            return

label_hostname(connection,args.hostname)
