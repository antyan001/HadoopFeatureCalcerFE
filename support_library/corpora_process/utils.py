import os
import json
import re
import string
import numpy as np
import pandas as pd


# text-preprocessing


def lower(text):
    return text.lower()


def remove_hashtags(text):
    clean_text = re.sub(r'#[A-Za-z0-9_]+', "", text)
    return clean_text


def remove_user_mentions(text):
    clean_text = re.sub(r'@[A-Za-z0-9_]+', "", text)
    return clean_text


def remove_urls(text):
    clean_text = re.sub(r'(https|http):\/\/[A-Za-z0-9_\.]+(?:\/)?[A-Za-z0-9_\.]+[\r\n]*', '', text, flags=re.MULTILINE)
    return clean_text

def striphtml(text):
    p = re.compile(r'<.*?>')
    return p.sub(' ', text)

def remove_punctuation(text):
    exclude = set(string.punctuation) 
    punc_free = ''.join(ch if ch not in exclude else ' ' for ch in text)
    return punc_free

def text_prepare(text):
    """
        text: a string
        
        return: modified initial string
    """
    ADD_TAGS_RE = re.compile('id|vk|com|video|wifi')
    DIGIT_SYMBOLS_RE = re.compile('\d+')

    text = DIGIT_SYMBOLS_RE.sub('', text)
    text = ADD_TAGS_RE.sub('', text)
    text = ' '.join(word for word in text.split() if len(word)>3)
    return text

preprocessing_setps = {
    'remove_hashtags': remove_hashtags,
    'remove_urls': remove_urls,
    'remove_user_mentions': remove_user_mentions,
    'remove_html': striphtml,
    'text_prepare': text_prepare,
    'remove_punctuation': remove_punctuation,
    'lower': lower
}

def normalize_text(steps, text):
    if steps is not None:
        for step in steps:
            text = preprocessing_setps[step](text)
    return text

def process_text(steps, text):
    if steps is not None:
        for step in steps:
            text = preprocessing_setps[step](text)

    processed_text = ""
    for word in text.split():
        processed_text += ' '.join(list(word)) + " "
    return processed_text

def margin_sentences(line, cutlenght=300, repeat_pattern='['']', returnlist = True):
    '''
        Arguments
        ---------
        line           - input text of type string or list 
        cutlenght      - number of tokens to remain within final list  
        repeat_pattern - pattern to pad abscent values of input line
                         ''     - means pad with empty strings (if we wanna joined string on the output)
                         '['']' - means to pad with list of empty strings (if we wanna list of strings on the output)
    '''
    if isinstance(line, str): 
        words_lst = line.split()
    elif isinstance(line, list) or isinstance(line, np.ndarray):
        words_lst = list(line)
    if len(words_lst) < cutlenght:
        if repeat_pattern=='['']':
            add_lst=list(map(list, np.repeat('',cutlenght-len(words_lst))[...,np.newaxis]))
            words_lst.extend(add_lst)
        else:
            words_lst.extend(list(np.repeat('',cutlenght-len(words_lst))))
    elif len(words_lst) > cutlenght:
        words_lst = words_lst[:cutlenght]
    if not returnlist and repeat_pattern!='['']':
        return ' '.join(words_lst)
    elif not returnlist and repeat_pattern=='['']': 
        return print('returned object is incompartible with the repeat_pattern value')
    else:    
        return words_lst

def extract_subsentences(line: str, num_part=3, ngrams=[2,3,4], cutlenght = None):
    '''
        Arguments
        ---------
        line     - input text of type string
        num_part - number of partitions to split on input line
        ngrams   - list of ngrams: len(ngrams) should be equal to the value of num_part 
                   parameter. Each ngram's value corresponds to the own partition. 
                   For example number of successive words from first partition will be taken 
                   according to ngrams[0] value 
                   1 --> ngrams[0]
                   2 --> ngrams[1]
                   ...
        cutlenght- parameter defines if we should cut excessive words in input text.
                   None means no cutting 
    '''
    split_sentence = []
    words_split = []
    
    words_lst = line.split()

    if cutlenght!=None:
        if len(words_lst) < cutlenght:
            words_lst.extend(list(np.repeat('',cutlenght-len(words_lst))))
        elif len(words_lst) > cutlenght:
            words_lst = words_lst[:cutlenght]
           
    splits = np.array_split(words_lst,num_part)
    iterator = iter(splits)
    item = next(iterator)
    words_split.append(item)

    for split in iterator:
        words_split.append(np.append(item,split))
        
    for i,split in enumerate(words_split):
        ngram_concat = []
        ngram_part = len(split)//ngrams[i]+1
        words_split = np.array_split(split,ngram_part)
        tmp=[]
        for ngrams_w in words_split:
            tmp.extend(ngrams_w)
            if len(tmp)==1:
                tmp.extend(np.repeat(tmp[0],2))
            ngram_concat.append(tmp.copy())
        split_sentence.extend(ngram_concat.copy())

    return split_sentence 


def extend(df, lst_cols, fill_value=None, preserve_index=False):
    # make sure `lst_cols` is list-alike
    if (lst_cols is not None
        and len(lst_cols) > 0
        and not isinstance(lst_cols, (list, tuple, np.ndarray, pd.Series))):
        lst_cols = [lst_cols]
    # all columns except `lst_cols`
    idx_cols = df.columns.difference(lst_cols)
    # calculate lengths of lists
    lens = df[lst_cols[0]].str.len()
    # preserve original index values    
    idx = np.repeat(df.index.values, lens)
    # create "exploded" DF
    res = (pd.DataFrame({
                col:np.repeat(df[col].values, lens)
                for col in idx_cols},
                index=idx)
             .assign(**{col:np.concatenate(df.loc[lens>0, col].values)
                            for col in lst_cols}))
    # append those rows that have empty lists
    if (lens == 0).any():
        # at least one list in cells is empty
        if fill_value is not None:
            res = (res.append(df.loc[lens==0, idx_cols])
                      .fillna(fill_value))
        else:
            res = (res.append(df.loc[lens==0, idx_cols]))
    # revert the original index order
    res = res.sort_index()
    # reset index if requested
    if not preserve_index:        
        res = res.reset_index(drop=True)
    return res