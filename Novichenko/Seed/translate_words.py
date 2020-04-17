#!/usr/bin/python3

# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--input',required=True)
parser.add_argument('--target_lang',required=True)
args = parser.parse_args()

# imports
import os
import requests
import sys
import time
from googletrans import Translator
translator = Translator()
import uuid

# get keys from environment
key_var_name = 'TRANSLATOR_TEXT_SUBSCRIPTION_KEY'
if not key_var_name in os.environ:
    raise Exception('Please set/export the environment variable: {}'.format(key_var_name))
subscription_key = os.environ[key_var_name]

endpoint_var_name = 'TRANSLATOR_TEXT_ENDPOINT'
if not endpoint_var_name in os.environ:
    raise Exception('Please set/export the environment variable: {}'.format(endpoint_var_name))
endpoint = os.environ[endpoint_var_name]

def translate(text,target_lang,source_lang):
    # If you encounter any issues with the base_url or path, make sure
    # that you are using the latest endpoint: https://docs.microsoft.com/azure/cognitive-services/translator/reference/v3-0-translate
    endpoint = 'https://api.cognitive.microsofttranslator.com/'
    path = '/translate?api-version=3.0'
    params = '&to='+target_lang+'&from='+source_lang
    constructed_url = endpoint + path  + params
    headers = {
        'Ocp-Apim-Subscription-Key': subscription_key,
        'Content-type': 'application/json',
        'X-ClientTraceId': str(uuid.uuid4())
    }
    # You can pass more than one object in body.
    body = [{
        'text' : text,
        #'to' : lang,
    }]
    request = requests.post(constructed_url, headers=headers, json=body)
    response = request.json()
    if request.status_code != 200:
        raise ValueError('response=',response)
    return response[0]['translations'][0]['text']

# prepare files
print(f'translating {args.input} into {args.target_lang}... ',end='',flush=True)
with open(args.input) as f:
    input_words = f.read()
source_lang = os.path.basename(args.input).split('.')[-1]
output_file = args.input.split('.')[0] + '.' + args.target_lang
try: 
    f = open(output_file,'x')
except FileExistsError:
    if os.stat(output_file).st_size == 0:
        f = open(output_file,'w')
        print('restarting... ',end='',flush=True)
    else:
        print('already exists')
        sys.exit(0)

# perform translation with an exponential backoff for failed attempts
#sleep=0
#while True:
    #try:
        #results = translator.translate(input_words,dest=args.target_lang).text
        #break
    #except:
        #sleep_time=30*2**sleep
        #print('  sleep_time=',sleep_time)
        #time.sleep(sleep_time)
        #sleep+=1
results = translate(input_words,args.target_lang,source_lang)
f.write(results)
print('done')
