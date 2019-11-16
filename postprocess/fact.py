import sqlalchemy

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_url_info

def url2article(connection,url):
    '''
    Returns the `id_articles` value that corresponds to a given url,
    or `None` if no article corresponds to the url.

    FIXME: This function should download the article if it's not already in the db.

    FIXME: Think more carefully about how to handle non-canonical urls.
    '''
    url_info=get_url_info(connection,url)
    print('id_urls=',url_info['id_urls'])
    sql=sqlalchemy.sql.text('''
        SELECT id_articles
        FROM articles 
        WHERE id_urls=:id_urls;
    ''')
    res=connection.execute(sql,{
        'id_urls':url_info['id_urls']
        })
    id_articles=res.first()[0]
    return id_articles


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

########################################
# FIXME: should these label functions find a better home?

from textblob import TextBlob

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
    #textstat.text_standard,
    polarity,
    subjectivity
    ]

def label_hostname(connection,hostname):

    # get next article from hostname
    sql=sqlalchemy.sql.text('''
        SELECT DISTINCT ON (title) * FROM (
            SELECT DISTINCT ON (text) id_articles,text,title
            FROM articles
            WHERE 
                hostname=:hostname AND
                pub_time is not null AND
                text is not null AND
                title is not null AND
                id_articles NOT IN (SELECT id_articles FROM SENTENCES)
            LIMIT 1
        ) AS t;
    ''')
    res=connection.execute(sql,{
        'hostname':hostname
        })
    id_articles,text,title=res.first()
    sentences=get_sentences(connection,id_articles,text=text,title=title)

    # do labeling
    for id_sentences,sentence in enumerate(sentences):
        for label_func in label_funcs:
            get_label(connection,id_articles,id_sentences,label_func,sentence=sentence)


################################################################################
# temporary testing stuff goes here

# database connection
print('connect to db')
db='postgres:///novichenkobot'
engine = sqlalchemy.create_engine(db, connect_args={'connect_timeout': 120})
connection = engine.connect()

#print('do the stuff')
#test_url='https://www.nknews.org/2012/12/the-top-ten-most-bizarre-rumours-to-spread-about-north-korea/'
#id_articles=url2article(connection,test_url)
#sentences=get_sentences(connection,id_articles)
#score=get_label(connection,id_articles,0,textstat.flesch_reading_ease)
#print('score=',score)

import datetime
i=0
while True:
    print(datetime.datetime.now(),f': i={i}')
    label_hostname(connection,'www.nknews.org')
    i+=1
