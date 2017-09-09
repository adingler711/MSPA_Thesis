import os
import re
import pandas as pd
import zipfile
import textract
from multiprocessing import Pool, cpu_count


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0,len(seq), size))


def convert_pdf_transcripts(args):
    
    [transcript_zips, bloomberg_transcript_path] = args
    
    ticker_iteration_full_df = pd.DataFrame()

    for transcript_folder in transcript_zips:
        transcript_files = os.listdir(bloomberg_transcript_path + '/'+transcript_folder)

        pdf_list = [pdf for pdf in transcript_files if '.pdf' in pdf]
        no_pdf_list = [pdf for pdf in transcript_files if '.pdf' not in pdf]
        #no_pdf_list = [pdf for pdf in no_pdf_list if '.bba' in pdf]
        for bba in no_pdf_list:
            os.remove(bloomberg_transcript_path +transcript_folder + '/' + bba)

        ticker_iteration_list = []
        for file_i in pdf_list:
            #file_i = 'SD000000002024251580.pdf'
            # parse the pdf file
            text = textract.process(bloomberg_transcript_path +transcript_folder + '/' + file_i)
            co_name = re.findall('(?<=Company Name: )(.*)', text)
            co_ticker = re.findall('Ticker: ([^ ]*)', text)
            date = re.findall('(?<=Date: )(.*)', text)
            event_description = re.findall('(?<=Event Description: )(.*)', text)

            if len(co_name) > 0 and len(co_ticker) > 0 and len(date) > 0 and len(event_description) > 0:
                co_name = co_name[0].replace(" ", "_")
                co_ticker = co_ticker[0].replace("/", "")
                date = date[0].replace("-", "_")
                event_description = event_description[0].replace(" ", "_").split('-' or '/')[0]

                filename_new = 'Data/bloomberg_earnings_press_releases/' + co_ticker + '_' + date + '_' + event_description + '.txt'
                ticker_iteration_list.append([co_name, co_ticker, date, event_description])

                fo = open(filename_new, "w")
                print co_name, date, event_description
                header_lines = []
                for i in xrange(0, len(text.splitlines())):  # len(text.splitlines())

                    line_text = text.splitlines()[i]  # .replace('\xe2\x80\xa2 ', '')

                    if i < 20:
                        if len(line_text) > 0:
                            header_lines.append(line_text)
                        fo.write(line_text)  # .encode('utf-8'))
                        fo.write("\n")
                        fo.write("\n")

                    if ((line_text not in header_lines) and
                            ('Company Name' not in line_text) and
                            ('Page' not in line_text)):
                        fo.write(line_text)  # .encode('utf-8'))
                        fo.write("\n")
                        fo.write("\n")
                fo.close()

                os.remove(bloomberg_transcript_path +transcript_folder + '/' + file_i)
                # file_parent = file_i.split('/')[0]
                # if file_parent in os.listdir(bloomberg_transcript_path):

                delete_check = os.listdir(bloomberg_transcript_path + '/' +transcript_folder)
                pdf_search = [pdf for pdf in delete_check if '.pdf' in pdf]

                if len(pdf_search) == 0:
                    os.removedirs(bloomberg_transcript_path + transcript_folder)

                ticker_iteration_df = pd.DataFrame(ticker_iteration_list)
                ticker_iteration_full_df = pd.concat((ticker_iteration_full_df, ticker_iteration_df))

            else:
                pass
                os.remove(bloomberg_transcript_path + transcript_folder + '/' + file_i)
                delete_check = os.listdir(bloomberg_transcript_path + '/' +transcript_folder)
                pdf_search = [pdf for pdf in delete_check if '.pdf' in pdf]

                if len(pdf_search) == 0:
                    os.removedirs(bloomberg_transcript_path + transcript_folder)

    return ticker_iteration_full_df

bloomberg_transcript_path = 'Data/bloomberg_transcripts/'
transcript_zips = os.listdir(bloomberg_transcript_path)
transcript_zips.remove('.ipynb_checkpoints')

for zipp in transcript_zips:
    zf = zipfile.ZipFile(bloomberg_transcript_path + zipp, 'r')
    zf.extractall(bloomberg_transcript_path)

dir_list = os.listdir(bloomberg_transcript_path)
dir_folders_list = [pdf for pdf in dir_list if '.' not in pdf]

if len(dir_folders_list) < cpu_count():
    use_cpu = len(transcript_zips)
else: 
    use_cpu = cpu_count()
    
pool = Pool(use_cpu)

input_list = []
for folder in chunker(dir_folders_list, len(dir_folders_list)/use_cpu):
    input_list.append([folder, bloomberg_transcript_path])

runs = pool.map(convert_pdf_transcripts, input_list) 
runs_concat = pd.concat(runs)
runs_concat.columns = ['company_name', 'ticker', 'date', 'event_description']
runs_concat.to_csv('Data/transcripts_collected.csv', index=False)

