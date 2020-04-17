# -*- coding: utf-8 -*-

'''
This file lists the languages available in bing translate.
'''

import os, requests, uuid, json
import pprint

endpoint = 'https://api.cognitive.microsofttranslator.com/'
path = '/languages?api-version=3.0'
constructed_url = endpoint + path
headers = {
    'Content-type': 'application/json',
    'X-ClientTraceId': str(uuid.uuid4())
}
request = requests.get(constructed_url, headers=headers)
response = request.json()

pprint.pprint(response['translation'])
