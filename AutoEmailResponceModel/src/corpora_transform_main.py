import os
import sys
import pandas as pd
import numpy as np

curruser = os.environ.get('USER')
sys.path.insert(0,'/home/{}/.local/lib/python3.5/site-packages/'.format(curruser))
sys.path.insert(0,'/opt/workspace/{}/libs/python3.5/site-packages/'.format(curruser))

from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from gensim.models.fasttext import FastText as FT_gensim
from gensim.corpora import Dictionary
from gensim import corpora
from gensim.models import TfidfModel
from gensim.test.utils import datapath
from gensim.utils import tokenize
from gensim.test.utils import common_texts
import smart_open

import string


# Iterator for FastText train
class GensimCorporaIter(object):
    def __init__(self, pathtofile):
        self.pathtofile = pathtofile
    def __iter__(self):
        path = datapath(self.pathtofile)  # absolute path to corpus
        with open(path, 'r', encoding='cp1251', errors = 'ignore') as fin:
            for line in fin:
                line = line.rstrip('\n')
                yield list(tokenize(line))

# Pipeline with FastText
class TfIdfFastTextEmbedVectorizer():

    def __init__(self, model,
                 word_ngram_range=(1,2),
                 char_ngram_range=(1,1),
                 char_max_vec_features=100,
                 word_max_vec_features=600,
                 useTfIdf2Pred=True,
                 smartstr= 'ntc'):
        self.model = model
        self.word_max_vec_features = word_max_vec_features
        self.char_max_vec_features = char_max_vec_features
        self.word_ngram_range      = word_ngram_range
        self.char_ngram_range      = char_ngram_range
        self.useTfIdf2Pred         = useTfIdf2Pred
        self.smartstr              = smartstr

    def set_params(self, **parameters):
        for parameter, val in parameters.items():
            setattr(self,parameter,val)

    def get_params(self, deep=True):
        return {'word_ngram_range'      : self.word_ngram_range,
                'char_ngram_range'      : self.char_ngram_range,
                'word_max_vec_features' : self.word_max_vec_features,
                'char_max_vec_features' : self.char_max_vec_features,
                'model'                 : self.model}

    def _gensimTfidfModel(self, corpora_cls):
        dct = Dictionary(corpora_cls)
        bow = [dct.doc2bow(line) for line in corpora_cls]
        tfIdfModel = TfidfModel(bow, smartirs=self.smartstr)
        dic = [(k,v) for k,v in dct.items()]
        rev_dct = dict(map(reversed, dic))
        self.gensim_rev_dct = rev_dct
        self.gensim_tfIdfModel = tfIdfModel
        self.bow = bow

    def _weight_words2vec(self):
        corpora_cls = []

        if self.isfit:
            for line in self.corpora:
                if len(line)!=0:
                    words = line.split()
                    new_line = [word for word in words if word in self.model.wv.vocab]
                    corpora_cls.append(new_line)
            self._gensimTfidfModel(corpora_cls)
            result = []
            for i,line in enumerate(corpora_cls):
                if len(line)!=0:
                    embed = []
                    weights = dict(self.gensim_tfIdfModel[self.bow[i]])
                    embed = [self.model[word]*weights[self.gensim_rev_dct[word]] for word in line]
                    mean_vec = np.array(embed).mean(axis=0)
                else:
                    mean_vec = np.zeros((self.model.vector_size,))
                result.append(mean_vec)
        else:
            for line in self.corpora:
                if len(line)!=0:
                    words = line.split()
                    new_line = [word for word in words if word in self.model.wv.vocab]
                    corpora_cls.append(new_line)
            result = []
            bow_full=[]
            for ele in self.bow:
                bow_full.extend(self.gensim_tfIdfModel[ele])
            weights = dict(bow_full)
            for i,line in enumerate(corpora_cls):
                if len(line)!=0:
                    embed = []
                    for word in line:
                        indx = self.gensim_rev_dct.get(word,None)
#                         print('word: {} index: {}'.format(word,indx))
                        if indx!=None:
                            embed.append(self.model[word]*weights[self.gensim_rev_dct[word]])
                        else:
                            embed.append(self.model[word])
                    mean_vec = np.array(embed).mean(axis=0)
                else:
                    mean_vec = np.zeros((self.model.vector_size,))
                result.append(mean_vec)

        dct = {indx: result[:][indx].tolist() for indx in range(len(result))}
        dfvect = pd.DataFrame.from_dict(dct, orient='index',columns=['_FT_{}'.format(i) for i in range(self.model.vector_size)])
        self.dfvect = dfvect

    def _TfIdfEmbeddingVectorizer(self):
        #tfidf join other model on top
        word_tfidf = TfidfVectorizer( sublinear_tf=False,
                                      strip_accents='unicode',
                                      analyzer='word',
                                      max_features=self.word_max_vec_features,
                                      ngram_range=self.word_ngram_range,
                                      max_df=2.0,
                                      min_df=0.001,
                                      norm = None )

        char_tfidf = TfidfVectorizer( sublinear_tf=False,
                                      strip_accents='unicode',
                                      analyzer='char',
                                      max_features=self.char_max_vec_features,
                                      ngram_range=self.char_ngram_range,
                                      max_df=2.0,
                                      min_df=0.001,
                                      norm = None )

        self.word_tfidf_sparce = word_tfidf.fit_transform(self.corpora)
        self.char_tfidf_sparce = char_tfidf.fit_transform(self.corpora)
        self.model_word_tfidf  = word_tfidf
        self.model_char_tfidf  = char_tfidf

    def fit(self,x,y=None):
        self.corpora = x
        self.isfit = True
        self._weight_words2vec()
        self._TfIdfEmbeddingVectorizer()
#         fasttext_weight_df = self.dfvect
#         out                = self.tfidf_sparce
#         tfidf_df = pd.DataFrame(out[:].todense(), columns=self.model_tfidf.get_feature_names())
#         df_res = pd.concat([tfidf_df,fasttext_weight_df], ignore_index=True, axis=1)
#         self.fit_df = df_res

        return self

    def transform(self,x):
        self.corpora = x
        self.isfit = False
        self._weight_words2vec()
        fasttext_weight_df = self.dfvect
        if self.useTfIdf2Pred:
          word_out           = self.model_word_tfidf.transform(x)
          char_out           = self.model_char_tfidf.transform(x)
          w_tfidf_df  = pd.DataFrame(word_out[:].todense(), columns=self.model_word_tfidf.get_feature_names()[:])
          ch_tfidf_df = pd.DataFrame(char_out[:].todense(), columns=self.model_char_tfidf.get_feature_names()[:])
          df_res = pd.concat([w_tfidf_df,ch_tfidf_df,fasttext_weight_df], ignore_index=True, axis=1)
          self.feature_names = w_tfidf_df.columns.values.tolist() + ch_tfidf_df.columns.values.tolist() + \
                               fasttext_weight_df.columns.values.tolist()
        else:
          df_res = fasttext_weight_df
          self.feature_names = fasttext_weight_df.columns.values.tolist()

        return df_res.values



