import re
import os
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from multiprocessing import Pool, cpu_count

import sys
reload(sys)
sys.setdefaultencoding("utf-8")


# Define a function get_paragraphs(file) that loops through the lines in the given text file, 
# collects the lines into paragraphs, and returns a simple list of paragraphs, 
# where each paragraph is a simple string.

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0,len(seq), size))


def create_paragraphs(lines):
    sent = []
    document = []
    prev_line_sent = True

    for line in lines:
        temp = sent_tokenize(line.decode('utf-8'))

        if prev_line_sent == True and len(temp) > 0:
            sent.append(temp[0])
            count = 0

        elif prev_line_sent == True and len(temp) == 0:
            count = 0
            prev_line_sent = False

        elif prev_line_sent == False and len(temp) == 0:
            if count < 2:
                count += 1 
            else:
                count += 1

        elif prev_line_sent == False and len(temp) > 0:
            if count < 2:
                sent.append(temp[0])
            else:
                document.append(sent)
                sent = []
                sent.append(temp[0])

            prev_line_sent = True
            count = 0  

    return document

def line_to_words(raw_text):
    
    letters_only = re.sub("[^a-zA-Z0-9]", " ", raw_text) 
    words = letters_only.split()                             
    stops = set(stopwords.words("english"))    
    meaningful_words = [w for w in words if not w in stops]
    
    return ( " ".join( meaningful_words ))

def clean_document(document):
    
    cleaned_documnet = []
    for para in document:
        paragraph = []
        str_paragraph = ' '.join(para)
        sentence_list = str_paragraph.split('. ')
        for sent in sentence_list:
            cleaned_sent = word_tokenize(line_to_words(sent).lower())
            cleaned_sent = ' '.join(cleaned_sent)
            paragraph.append(cleaned_sent)
        str_paragraph_cleaned = '. '.join(paragraph)
        cleaned_documnet.append(str_paragraph_cleaned)
        
    return cleaned_documnet

def cleane_text_files(temp):
    
    for text_file in temp:
        text_data = open('Data/bloomberg_earnings_press_releases/' + text_file, "r")
        lines = text_data.readlines()

        document = create_paragraphs(lines)
        cleaned_documnet = clean_document(document)

        filename_new = 'Data/bloomberg_earnings_press_releases_cleaned/' + text_file

        fo = open(filename_new, "w")
        print text_file

        for para in cleaned_documnet: 
            print para
            question_section = re.findall('(?<=q a)(.*)', para)
            if len(question_section) < 1:
                fo.write(para)
                fo.write("\n")
                fo.write("\n")
            else:
                fo.write("question section")
                fo.write("\n")
                fo.write("\n")
                fo.write(para)
                fo.write("\n")
                fo.write("\n")

        fo.close()
        
bloomberg_transcript_path = 'Data/bloomberg_earnings_press_releases/' 
bloomberg_text_files = os.listdir(bloomberg_transcript_path)
bloomberg_text_files.remove('.ipynb_checkpoints')

if len(bloomberg_text_files) < cpu_count():
    use_cpu = len(bloomberg_text_files)
else: 
    use_cpu = cpu_count()
    
pool = Pool(use_cpu)

input_list = []

for folder in chunker(bloomberg_text_files, len(bloomberg_text_files)/use_cpu):
    input_list.append(folder)
    
runs = pool.map(cleane_text_files, input_list)