import codecs
import nltk, string
import pandas as pd
import re
import sys
import os
import pandas as pd
import urllib3
import json
import requests
from glob import iglob
from datetime import datetime
import sys
import csv
import unicodecsv as csv
regex = re.compile(r'[\n\r\t]')

maxInt = sys.maxsize
decrement = True

while decrement:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt/10)
        decrement = True


def clean_text(data):
    """Parse the raw latin texts.
    Parsing by period
    :return: sentence id: sentences
    """
    data = regex.sub(' ', data)
    return data

def create_csv():
    """Create a csv file of parsed data
    :return: list of dictionaries for pased data
    """
    template= {"author":"", "filename":"","title":"","sentence":"",  "url":""}

    rows = []

    for path, subdirs, files in os.walk('latin_text_latin_library/'):
        doc = template

        for name in files:
            directory = os.path.join(path, name)
            print(directory)
            ##directory = 'data/tertullian/tertullian.idololatria.txt'
            # check if files are under a folder

            if(len(directory.split('/')) >= 3):
                # skip to next iteration if the directory is for the hidden folder
                if path.split('/')[1][0] == '.':
                    continue
                else:
                    doc['author'] = directory.split('/')[1]
                    doc['filename'] = directory.split('/')[2]
                    subdirectory = re.sub('.txt', '', doc['filename'])
                    doc['url'] = "http://thelatinlibrary.com/"+ doc['author']+"/"+subdirectory+".html"

                    # check if the url opens the page without any error
                    req = requests.get(doc['url'])

                    # if the request is bad, change the url ending from '.html' to '.shtml'
                    if req.status_code > 400:
                        doc['url'] = "http://thelatinlibrary.com/" + doc['author'] + "/" + subdirectory + ".shtml"

            else:
                doc['filename'] = directory.split('/')[-1]
                author = re.sub('.txt', '', doc['filename'])
                if author[0] == '.':
                    continue
                else:
                    doc['author'] = author
                    doc['url'] = "http://thelatinlibrary.com/" + doc['author'] + ".html"
                    req = requests.get(doc['url'])
                    if req.status_code > 400:
                        doc['url'] = "http://thelatinlibrary.com/" + doc['author'] + ".shtml"
            try:
                with codecs.open(directory, "r",encoding='utf-8', errors='ignore') as f1:
                    data = f1.read()
                # call cleaning text function

                doc['sentence']= clean_text(data)

                # assuming the title is listed as the first sentence of the text file
                #doc['title']= re.split(r'\s{2,}', doc['sentence'][1].decode('utf-8'))[0]

                ##es.index(index="latin", doc_type='library', body=doc, request_timeout=60)
            except Exception as exp:
                raise exp
                print("ERROR: {0}: {1}".format(filename,exp))

            rows.append(doc.copy())
    df = pd.DataFrame(rows)
    df.to_csv("staging_data.csv", encoding = 'utf-8')

if __name__ == '__main__':
    create_csv()