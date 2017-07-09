# Generate the list of index files archived in EDGAR since start_year (earliest: 1993) until the most recent quarter
import re
import datetime
import numpy as np
import pandas as pd
import requests

current_year = datetime.date.today().year
current_quarter = (datetime.date.today().month - 1) // 3 + 1
start_year = 1993  # EDGAR started archiving data starting in 1993
years = list(range(start_year, current_year))
quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
headers = ['cik', 'company', 'type', 'date', 'path']
history = [(y, q) for y in years for q in quarters]
output_df = pd.DataFrame()

for i in range(1, current_quarter + 1):
    history.append((current_year, 'QTR%d' % i))
urls = ['https://www.sec.gov/Archives/edgar/full-index/%d/%s/master.idx' % (x[0], x[1]) for x in history]
urls.sort()

for yr in years:
    datakeep = []
    output_array = np.zeros(len(headers))

    for line in urls:
        if re.search(str(yr), line) and not re.search('NT', line):
            datakeep.append(line)
        else:
            pass

    for url in datakeep:
        lines = requests.get(url).text.splitlines()
        records = [tuple(line.split('|')) for line in lines[11:]]
        # the headers within each tuple are cik, conm, type, date, path
        output_array = np.vstack((output_array, records))
        print(url, 'downloaded and wrote to array')
        output_df = pd.DataFrame(output_array[1:], columns=headers)

    datakeep = []
    for line in np.unique(output_df['type']):
        if re.search('10[ -]?[kKqQ]', line) and not re.search('NT', line):
            datakeep.append(line)
        else:
            pass

    output_df = output_df[output_df['type'].isin(datakeep)]
    output_df.to_csv('Data/Edgar_master/master_idx_' + str(yr) + '.csv.gz', index=False, compression='gzip')