import os
import pandas as pd
import numpy as np
import re
import string
import sys

#import pymorphy2
import nltk
#import seaborn as sns
#import matplotlib.pyplot as plt
from nltk.corpus import stopwords
from pymorphy2.analyzer import MorphAnalyzer
from nltk.tokenize import RegexpTokenizer, word_tokenize
from multiprocessing import Pool
from functools import partial
from tqdm import tqdm
#tqdm.pandas()

class Preproc(object):
    
    def __init__(self):
        self.stopwords = set(stopwords.words('russian'))
        self.exclude = set(string.punctuation) 
        self.exclude = self.exclude.difference(['"',"'"])
        self.stemmer = MorphAnalyzer()
        self.lemmatize = False
        
    @staticmethod
    def text_prepare(text, 
                     stopwords, 
                     lemmatize, 
                     stemmer, 
                     exclude):           
        text = ''.join(x if x not in exclude else ' ' for x in text)
        text = ' '.join(word for word in text.split() if word not in stopwords)
        text = re.sub(r'[a-zA-Z\d{1,8}]+','',text)
        text = ' '.join(word for word in text.split() if len(word)>2) 
        tokenslst = list(map(lambda x: x.replace('"', '').lower(), RegexpTokenizer('([а-яА-Яa-zA-Z\d{9,12}]+|\"[\w\s]+\")').tokenize(text)))
        if not lemmatize:
            text = ' '.join(tokenslst)
        else:    
            text = ' '.join(stemmer.normal_forms(x)[0] for x in tokenslst)
        return text
    
    @staticmethod
    def clean_df(df, 
                 clean_func, 
                 stopwords, 
                 lemmatize, 
                 stemmer, 
                 exclude):
        df['text_norm'] = df['text'].progress_apply(lambda x: clean_func(x, 
                                                                         stopwords, 
                                                                         lemmatize, 
                                                                         stemmer, 
                                                                         exclude))
        return df

    def parallelize_df(self,df,num_part=10, num_workers=10):
        df_split = np.array_split(df,num_part)
        pool = Pool(num_workers)
        df = pd.concat(pool.map(partial(Preproc.clean_df,
                                        clean_func = Preproc.text_prepare, 
                                        stopwords  = self.stopwords, 
                                        lemmatize  = self.lemmatize, 
                                        stemmer    = self.stemmer,
                                        exclude    = self.exclude), df_split))
        pool.close()
        pool.join()
        return df


