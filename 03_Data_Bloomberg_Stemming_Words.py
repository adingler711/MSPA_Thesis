import nltk
import os
from nltk.tokenize import PunktSentenceTokenizer
from multiprocessing import Pool, cpu_count
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet

import sys

reload(sys)
sys.setdefaultencoding("utf-8")


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def process_content(tokenized):
    try:
        words = nltk.word_tokenize(tokenized)
        tagged = nltk.pos_tag(words)
        return tagged

    except Exception as e:
        print(str(e))


def get_wordnet_pos(row):
    if row[1].startswith('J'):
        return wordnet.ADJ
    elif row[1].startswith('V'):
        return wordnet.VERB
    elif row[1].startswith('N'):
        return wordnet.NOUN
    elif row[1].startswith('R'):
        return wordnet.ADV
    else:
        return ''


def stem_words(pos_tags):
    new_line = []
    for row in pos_tags:
        pos_updated = get_wordnet_pos(row)
        if len(pos_updated) > 0:
            stemmed_word = wordnet_lemmatizer.lemmatize(row[0], pos_updated)
            new_line.append(stemmed_word)
        else:
            stemmed_word = wordnet_lemmatizer.lemmatize(row[0])
            new_line.append(stemmed_word)
    return new_line


def stem_document(bloomberg_text_files):
    for text_file in bloomberg_text_files:
        text_data = open('Data/bloomberg_earnings_press_releases_cleaned/' + text_file, "r")
        lines = text_data.readlines()

        stemmed_document = []
        for line in lines:

            if line != '\n':
                tokenized = tokenizer.tokenize(line)
                # iterate by token sentence!
                for sent_row in tokenized:
                    pos_tags = process_content(sent_row)
                    stemmed_line = stem_words(pos_tags)
                    str_sent = ' '.join(stemmed_line)
                    stemmed_document.append(str_sent)
            else:
                stemmed_document.append(line)

        filename_new = 'Data/bloomberg_earnings_press_release_stemmed/' + text_file

        fo = open(filename_new, "w")
        for sent in stemmed_document:
            fo.write(sent)
            fo.write("\n")
        fo.close()


bloomberg_transcript_path = 'Data/bloomberg_earnings_press_releases_cleaned/'
bloomberg_text_files = os.listdir(bloomberg_transcript_path)
bloomberg_text_files.remove('.ipynb_checkpoints')

# removes punctuation and returns list of words
# tokenizer = RegexpTokenizer(r'\w+')
tokenizer = PunktSentenceTokenizer()
wordnet_lemmatizer = WordNetLemmatizer()

if len(bloomberg_text_files) < cpu_count():
    use_cpu = len(bloomberg_text_files)
else:
    use_cpu = cpu_count()

pool = Pool(use_cpu)

input_list = []
for chunk in chunker(bloomberg_text_files, len(bloomberg_text_files) / use_cpu):
    input_list.append(chunk)

pool.map(stem_document, input_list)