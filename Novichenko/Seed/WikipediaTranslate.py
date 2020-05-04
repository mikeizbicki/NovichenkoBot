import requests
from bs4 import BeautifulSoup, SoupStrainer
from urllib.parse import unquote, urlparse

#url = 'https://en.wikipedia.org/wiki/North_Korea'
url = 'https://en.wikipedia.org/wiki/Donald_Trump'
#url = 'https://en.wikipedia.org/wiki/Kim_Jong-un'
response = requests.get(url)
print('response=',response)

bs = BeautifulSoup(response.text, features='lxml')

translations = {}
for link in bs.find_all('a',{'class':'interlanguage-link-target'},href=True,):
    if link['href'][:8]=='https://':
        p = urlparse(link['href'])
        lang = p.hostname.split('.')[0]
        trans = unquote(p.path.split('/')[2]).replace('_',' ')
        translations[lang] = trans

for lang in sorted(translations.keys()):
    print(lang,':',translations[lang])
