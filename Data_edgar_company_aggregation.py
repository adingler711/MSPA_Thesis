import pandas as pd
import requests
import os


def write_files(filing):
    tik = filing['Ticker']
    date = filing['date'].replace('-', '_')
    url = 'https://www.sec.gov/Archives/' + filing['path']

    with open('Data/Filings/' + tik + '_' + date + '.txt', 'wb') as f:
        f.write(requests.get('%s' % url).content)
        print(url, 'downloaded and wrote to text file')


master_idx_path = 'Data/Edgar_master/'
REIT_CO = pd.read_csv('Data/REIT_Table_cik.csv')

for filename in os.listdir(master_idx_path):
    if filename[0] == 'm':
        master_idx = pd.read_csv(master_idx_path + filename)
        REIT_df = pd.merge(master_idx, REIT_CO, left_on='cik',
                           right_on='CIK', how='inner').drop(['CIK', 'company'], axis=1)
        REIT_df.apply(write_files, axis=1)

    else:
        pass