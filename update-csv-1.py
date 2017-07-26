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
import math
import unicodecsv as csv
from elasticsearch import Elasticsearch
es = Elasticsearch(['localhost:9200'])

regex = re.compile(r'[\n\r\t]')


def give_sentence_ID(data):
    """Parse the raw latin texts.
    Parsing by period
    :return: sentence id: sentences
    """
    tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    sentences = tokenizer.tokenize(data)
    cleaned_sentences = [x.encode('utf-8') for x in sentences if x != "."]
    sentences_id = {i: line for i, line in enumerate(cleaned_sentences, 1)}
    return sentences_id

def update_push_to_ES():
    """pase raw text files
    :return: list for ES, list for creating a csv file
    """
    staging_data = pd.read_csv("staging_data.csv")
    staging_data.isnull().sum()

    input = pd.read_csv("input_file.csv")

    staging_data.drop(staging_data.columns[[0]], axis=1, inplace=True)
    staging_data['author'] = input['author']
    staging_data['filename'] = input['filename']
    staging_data['title'] = input['title']

    docs = staging_data.to_dict('records')

    for i, row in enumerate(docs):
        filename = row['filename']
        data = row['sentence']

        if pd.isnull(row['sentence']):
            print("Sentence is empty at",i, filename)
            continue
        else:
            data = data.decode('utf-8')
            try:
                docs[i]['sentence'] = give_sentence_ID(data)
                res = es.index(index="latin", doc_type='library', body=docs[i], request_timeout=60)
                print(i, filename, " inserted: ",res['created'])
            except Exception as exp:
                raise exp
                print("ERROR: {0}: {1}".format(filename, exp))


if __name__ == '__main__':
    update_push_to_ES()