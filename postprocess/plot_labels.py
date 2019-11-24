#! /usr/bin/python3

# command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',type=str,default='postgres:///novichenkobot')
parser.add_argument('--hostnames',type=str,nargs='+',required=True)
parser.add_argument('--queries',type=str,nargs='+',default=[])
parser.add_argument('--title_only',action='store_true')
args = parser.parse_args()

# import libraries
import sqlalchemy
import datetime
import matplotlib.pyplot as plt
import mpld3
import json
import numpy as np

# hack for getting mpld3 to work
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        import numpy as np
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)
from mpld3 import _display
_display.NumpyEncoder = NumpyEncoder

# database connection
print('connect to db')
engine = sqlalchemy.create_engine(args.db, connect_args={'connect_timeout': 120})
connection = engine.connect()

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
from NovichenkoBot.sqlalchemy_utils import get_url_info,reverse_hostname
from NovichenkoBot.spiders.general_spider import GeneralSpider

def is_outlier(points, thresh=5):
    """
    Returns a boolean array with True if points are outliers and False
    otherwise.

    Parameters:
    -----------
        points : An numobservations by numdimensions array of observations
        thresh : The modified z-score to use as a threshold. Observations with
            a modified z-score (based on the median absolute deviation) greater
            than this value will be classified as outliers.

    Returns:
    --------
        mask : A numobservations-length boolean array.

    References:
    ----------
        Boris Iglewicz and David Hoaglin (1993), "Volume 16: How to Detect and
        Handle Outliers", The ASQC Basic References in Quality Control:
        Statistical Techniques, Edward F. Mykytka, Ph.D., Editor.
    """
    if len(points.shape) == 1:
        points = points[:,None]
    median = np.median(points, axis=0)
    diff = np.sum((points - median)**2, axis=-1)
    diff = np.sqrt(diff)
    med_abs_deviation = np.median(diff)

    modified_z_score = 0.6745 * diff / (1e-6+med_abs_deviation)

    return modified_z_score > thresh

# get query results
def gen_plot(hostnames,id_labels,queries=None,title_only=True,alpha=0.99,dpi=96):

    print(datetime.datetime.now(),f'gen_plot(hostnames={hostnames},id_labels={id_labels})')
    fig, ax = plt.subplots(figsize=(800/dpi, 300/dpi), dpi=dpi)

    if type(hostnames) != list:
        hostnames = [hostnames]

    if queries is None or queries==[]:
        queries = [None]

    if type(queries) != list:
        queries = [queries]

    colors=['b','g','r','c','m']
    styles=['-','--',':','-.']

    for hostname,color in zip(hostnames,colors):
        for query,style in zip(queries,styles):

            # parse query
            query_parsed=''
            if query is not None:
                query_parsed = '&'.join(query.split(' '))

            # get data from db
            clause_title_only=''
            if title_only:
                clause_title_only='and labels.id_sentences=0'

            clause_query=''
            if query is not None:
                clause_query = 'and to_tsvector(sentence_resolved) @@ to_tsquery(:query)'

            sql=sqlalchemy.sql.text(f'''
            select id_labels,score,pub_time,id_urls_canonical,title
            from labels
            inner join get_valid_articles(:hostname) as valid on valid.id_articles = labels.id_articles
            inner join sentences on sentences.id_articles=labels.id_articles and sentences.id_sentences=labels.id_sentences
            where
                id_labels=:id_labels
                {clause_title_only}
                {clause_query}
            order by pub_time
            ;
            ''')
            res=connection.execute(sql,{
                'hostname':hostname,
                'id_labels':id_labels,
                'query':query_parsed
                })
            rows=list(res)
            print(f'  {hostname} : len(rows)={len(rows)}')

            # plot individual articles
            X = np.array([ row[2] for row in rows ])
            Y = np.array([ row[1] for row in rows ])
            outliers = is_outlier(Y)
            X_mod = X[~outliers]
            Y_mod = Y[~outliers]
            scatter = plt.scatter(X_mod, Y_mod, s=0.5, alpha=0.1, c=color)

            # plot running average
            Y_ave = [np.mean(Y[:100])]
            norm = [0]
            for i in range(len(Y)):
                norm_next = alpha+(1-alpha)*norm[i]
                norm.append(norm_next)
                Y_next = (alpha*Y_ave[-1] + (1-alpha)*Y[i])/norm_next
                Y_ave.append(Y_next)
            Y_ave=Y_ave[1:]
            plt.plot(X,Y_ave,c=color,linestyle=style)

    # save figure
    if len(hostnames)>1:
        plt.legend(hostnames,loc='upper right')
    plt.ylabel(id_labels)
    plt.tight_layout()
    output_filename=f'{list2str(hostnames)}_{list2str(queries)}_{id_labels}.png'
    plt.savefig('outputs/'+output_filename)
    return output_filename

def list2str(xs):
    xs = [ x if x is not None else 'None' for x in xs ]
    return ','.join(xs)

def plot_hostnames(hostnames,queries,**kwargs):
    if type(hostnames) != list:
        hostnames=[hostnames]

    labels=[
        'polarity',
        'subjectivity',
        'lexicon_count',
        'flesch_reading_ease',
        #'smog_index',
        'flesch_kincaid_grade',
        'coleman_liau_index',
        #'automated_readabilitiy_index',
        'dale_chall_readability_score',
        'difficult_words',
        'linsear_write_formula',
        'gunning_fog'
    ]
    with open(f'outputs/{list2str(hostnames)}_{list2str(queries)}.html','w') as f:
        f.write(f'<p><strong>hostnames:</strong>{list2str(hostnames)}</strong></p>')
        f.write(f'<p><strong>queries:</strong>{list2str(queries)}</strong></p>')
        for label in labels:
            pngfile=gen_plot(
                    hostnames,
                    label,
                    queries=queries,
                    **kwargs
                    )
            f.write(f'<p><img src="{pngfile}"></p>')

plot_hostnames(
        hostnames=args.hostnames,
        queries=args.queries,
        title_only=args.title_only
        )
