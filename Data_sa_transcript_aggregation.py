import time
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup

ticker_l = list(pd.read_csv('Data/REIT_Table_cik.csv')['Ticker'])
headers = {'User-Agent': 'Mozzila/5.0'}

for tiker in ticker_l:

    url = 'http://seekingalpha.com/symbol/' + tiker + '/earnings/transcripts'
    response = requests.get(url, headers=headers)

    if response.ok:

        soup = BeautifulSoup(response.text, 'html.parser')

        for link in soup.find_all('a'):
            x = link.get('href')

            if isinstance(x, basestring):
                wordlist = ['article', 'transcript']
                if all(x.find(s) >= 1 for s in wordlist):
                    parse_site = 'http://seekingalpha.com/' + x + '?part=single'
                    print parse_site
                    year = re.findall('\S-([0-9]*)-', parse_site)[0]
                    quarter = re.findall('\S-(q[0-9]*)-', parse_site)[0]
                    parse_req = requests.get(parse_site, headers=headers)
                    parse_soup = BeautifulSoup(parse_req.text, 'html.parser')
                    transcript = parse_soup.find(id="a-body")

                    fo = open('Conference_Earning_Calls/' + tiker + '_' + year + '_' + quarter + '_call.txt', "w")
                    callstart = True
                    for i in transcript.children:
                        if i.string == "Question-and-Answer Session":
                            callstart = False
                        if callstart:
                            t = i.string
                            if t:
                                fo.write(t.encode('utf-8'))
                                fo.write("\n")
                    fo.close()

                    fo = open('Conference_Earning_Calls/' + tiker + '_' + year + '_' + quarter + '_qanda.txt', "w")
                    qastart = False
                    for i in transcript.children:
                        if i.string == "Question-and-Answer Session":
                            qastart = True
                        if qastart:
                            t = i.string
                            if t:
                                fo.write(t.encode('utf-8'))
                                fo.write("\n")
                    fo.close()
                    time.sleep(2)

                    # break
        break