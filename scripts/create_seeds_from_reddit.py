#!/bin/python3

# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--outdir',default='inputs/reddit_seeds')
parser.add_argument('--reddit_dir',default='/data/files.pushshift.io/reddit/submissions')
args = parser.parse_args()

# imports
import datetime
import os
import gzip
import bz2
import lzma
import simplejson as json

# open output file
os.makedirs(args.outdir, exist_ok=True)

# loop through reddit files
num_urls=0
for filename in sorted(os.listdir(args.reddit_dir)):

    filepath=os.path.join(args.reddit_dir,filename)
    print(datetime.datetime.now(),f'num_urls={num_urls}, filename={filename}')

    # open the file using the appropriate compression
    fin = None
    if filename[-4:]=='.bz2':
        fin = bz2.open(filepath)
        basename = filename[:-3]
    if filename[-3:]=='.gz':
        fin = gzip.open(filepath)
        basename = filename[:-2]
    if filename[-3:]=='.xz':
        fin = lzma.open(filepath)
        basename = filename[:-2]
    if fin is None:
        continue

    outfile = os.path.join(args.outdir,basename)
    if os.path.exists(outfile):
        print('  file exists, skipping')
        continue

    # loop through file
    try:
        with open(outfile,'xt') as fout:
            for line in fin:
                import pprint
                try:
                    submission=json.loads(line)
                except:
                    print('  JSON failed to parse')
                #pprint.pprint(submission)

                subs_nk=['northkorea','northkoreanews','link','northkoreapics','hrnk']
                #subs_asia=['asia','eastasianews','japannews','korea','southkorea']
                #subs_conflict=['conflictnews','iraqconflict','syriancivilwar','ukranianconflict','yemencrisis']
                phrases=['north korea','dprk','kim il sung','kim ilsung','kim jong il','kim jongil','kim jong un','kim jongun','kim jong nam']

                try:
                    if ( submission['subreddit'].lower() in subs_nk or 
                         any([phrase in submission['title'].lower() for phrase in phrases])
                       ):
                        fout.write(submission['url']+'\n')
                        num_urls+=1
                except:
                    pass
                    #pprint.pprint(submission)
                    #print('no subreddit/title in json')
    except EOFError:
        print('  EOFError')

    # close the file
    fin.close()
