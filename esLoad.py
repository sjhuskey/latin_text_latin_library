import codecs
import nltk, string
import pandas as pd
import re
import sys
import os
import pandas as pd
import json
import requests
from glob import iglob
from datetime import datetime
import sys
import csv
import math
import unicodecsv as csv
import numpy as np
import csv

from elasticsearch import Elasticsearch
es = Elasticsearch(['localhost:9200'])

regex = re.compile(r'[\n\r\t]')

def getDirectoryList():
    """
    Create a root directory list
    :param rootDir:
    :return: directorylist
    """
    rootDir = 'data/'  ## rootdir is a folder name for the file repo
    directorylist= []

    for path, subdirs, files in os.walk(rootDir): # goes through file paths for every file in the root directory
        for name in files:
            directory = os.path.join(path, name) # makes a full directory

            if (len(directory.split('/')) >= 3):
                # skip to next iteration if the directory is for the hidden folder
                if path.split('/')[1][0] == '.':
                    continue
                else:
                    directorylist.append(directory)
    return directorylist # list of all directories


def readTexts(directorylist, filename):
    """
    Match the filename and get the directory for that file, open the file and read texts
    :param directorylist:
    :param filename:
    :return:
    """
    try:
        index = [i for i, x in enumerate(directorylist) if x.split('/')[-1] == filename] # index gets the row id(locations in the list) in the directory list
        if index ==[]: # if the filename cannot be found from the directory list
            textsblob = "" # puts empty string
        else: # if filename is found from the directory list
            filepath = directorylist[index[0]] # grabs the file path for the file
            if(os.path.isfile(filepath)): # check if there is the file exist
                with codecs.open(filepath, 'r', encoding='utf-8', errors='ignore') as f: # if true, open the file
                    textsblob = f.read() # read the raw texts from the file
            else: # if there is no file that exists
                textsblob = "" # put empty string
    except Exception as exp:
        raise exp
        print("ERROR: {0}: {1}".format(filename, exp))
        textsblob = "" # if index cannot be found, put an empty string
    return textsblob

def tokenizeSentences(data):
    """
    # Tokenize the file text the raw latin texts. Parsing by period
    :return: sentences
    """
    tokenizer = nltk.data.load('tokenizers/punkt/english.pickle') # nltk tokenizer
    sentences = tokenizer.tokenize(data)  # parse sentences by period
    cleaned_sentences = [x for x in sentences if x != "."] # if the sentence has only '.', remove it from the list

    # TO-DOS: # Clean the text some more

    return cleaned_sentences # return the list of sentences

def es_load(input, index="latin",doctype="library"):
    """
    Convert input dataframe to dictionary,
    Iterate dictionary and index each sentence in each file
    :return: None
    """
    docs = input.to_dict('records') # Converts the data frame to  dictionary as Elastic search takes the JSON format
    checklist = [] # error checking list, remove it later

    for i, row in enumerate(docs): # iterate over each file
        filename = row['filename']
        data = row['sentences']
        author = row['author']
        title = row['title']
        url = row['url']
        sentences = tokenizeSentences(data) # call function to tokenize the textblob to a list of sentences

        if(sentences == []): # if the sentences(textblob) is empty
            doc = {"author": author, "filename": filename, "title": title, # create a doc without the sentnece id, put empty string to the sentence field
                   "sentence": "", "url": url}
            checklist.append(doc)
            res = es.index(index=index, doc_type=doctype, body=doc, request_timeout=60) # push to ES
            print(i, filename, " inserted: ", res['created']) # print out the index result
            print("Sentence is empty at", i, filename)
        else:
            for (esindex, esrow) in enumerate(sentences): # iterate over  sentences in each file
                # for loop. index the each sentence to Elasticsearch
                doc = {"author": author, "filename": filename, "title": title, "sentence_id": esindex+1, # create a doc to be inserted
                       "sentence": esrow, "url": url}
                try:
                    res = es.index(index=index, doc_type=doctype, body=doc, request_timeout=60) # push to Elastic search
                    checklist.append(doc)
                    print(i, filename, " inserted: ",res['created'])
                except Exception as exp:
                    raise exp
                    print("ERROR: {0}: {1}".format(filename, exp))

if __name__ == '__main__':
    inputfile = sys.argv[1]

    input = pd.read_csv(inputfile, encoding="ISO-8859-1")  # Read meta from input file
    directorylist = getDirectoryList() # returns the list which has directories for all files

    for index, row in input.iterrows(): # iterate over the input file
        filename = row['filename'] # get the filename in each row
        input.loc[index, 'sentences'] = readTexts(directorylist, filename) # create a new column 'sentences' , add 'textblob' to the column

    # ignore the last column since it is a comment column that sam added
    input = input.iloc[:, :-1]

    # null value quality check
    input.isnull().sum() # check for the null value in the data frame

    #filename quality check
    for index, row in input.iterrows(): # iterate over the input file
        filename = row['filename'] # get the filename in each row
        if (filename.endswith('txt') == False):
            print("Wrong file name is inserted at: ",index,filename)

    # There are some null values from the file, check with Sam
    es_load(input)  # loads data to Elastic search
