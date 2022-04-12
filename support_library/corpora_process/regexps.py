import re
import os
import string
import numpy as np
from collections import OrderedDict
# text=preprocessing


def split_on_uppercase(s, keep_contiguius = True):
    
    
    is_lower_arround = (lambda: s[i-1].islower() or 
                        string_len > (i + 1) and s[i + 1].islower())

    is_lower_after_upper = (lambda: s[i-1].isupper() and 
                            string_len > (i + 1) and s[i + 1].islower())    
    
    parts = []
    
    words = s[0].split()
    for s in words:
        start = 0
        string_len = len(s)
        for i in range(1, string_len):
            if s[i].isupper() and (not keep_contiguius or is_lower_arround()) and (not is_lower_after_upper()):
                parts.append(s[start: i])
                start = i
            elif s[i].isupper() and is_lower_after_upper():
                parts.append(s[start: i+1])
                start = i+1            
        parts.append(s[start:]) 
    
    
    return ' '.join(parts)



def find_instagram(instr, sub=False):
  
    finalmatch=[]
    re_find_insta = re.compile(r'(?:instagram|инстаграм)(?:[: [|\]*?[A-Za-z0-9_\|.]+?[ |\]]*)@[A-Za-z0-9_.-]+(?:[ ,]*@[A-Za-z0-9_.-]+)?', re.UNICODE)
    
    instr = [multdotrep(instr)]
   
    if not sub:
        for word in instr:
          res=re_find_insta.findall(word)
          if res is not None:
            joinstr=[', '.join([g]) for g in res if len(g)!=0]
            if len(joinstr)!=0:
              finalmatch.extend(joinstr)    
            
        if len(finalmatch)!=0:
          return finalmatch
        else:
          return [''] 
    else:
      for word in instr:
        out = re_find_insta.sub(' ',word)
      return out
        
#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------
def find_email(instr, sub=False):
    
    finalmatch=[]
    re_find_email   = re.compile(r'[A-Za-z0-9_\.\-]+@\w+\.(?:com|ru)', re.UNICODE)
    dotrem = re.compile(r'^\.{1,}')
    
    instr = [multdotrep(instr)]
    
    if not sub:      
        for word in instr:
          res=re_find_email.findall(word)
          if res is not None:
            joinstr=[', '.join([dotrem.sub('',g)]) for g in res if len(g)!=0]
            if len(joinstr)!=0:
              finalmatch.extend(joinstr)    
            
        if len(finalmatch)!=0:
          return finalmatch
        else:
          return [''] 
    else:
      for word in instr:
        out = re_find_email.sub(' ',word)
      return out    

#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------
def find_url(instr, sub=False):
    finalmatch=[]
    htmlrem = re.compile(r'html')
    re_find_url   = re.compile(r'(?:https:|http:|plus.google.com|youtube.com|vk.com/topic|vk.com/album)[А-Яа-яA-Za-z0-9_=.-/\?%*&]+', re.UNICODE)
        
    instr = [multdotrep(instr)]    
    instr = [htmlrem.sub(',',instr[0])]
    
    if not sub:
        for word in instr:
          res=re_find_url.findall(word)
          if res is not None:
            joinstr=[', '.join([g]) for g in res if len(g)!=0]
            if len(joinstr)!=0:
              finalmatch.extend(joinstr)    
            
        if len(finalmatch)!=0:
          return finalmatch
        else:
          return [''] 
    else:
      for word in instr:
        out = re_find_url.sub(' ',word)
      return out    
                   
#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------    
def find_skype(instr, sub=False):
    matchstr=''
    finalmatch=[]    
    #remspace=re.compile(r'\s+')
    re_find_skype = re.compile(r'(?:skype|скайп)(?:[: ,]*)([A-Za-z0-9_.-]+)(?:[: ,]*)', re.UNICODE)
    #re_find_skype0 = re.compile(r'.*skype\:? ([A-Za-z0-9_\.]+(?:\s*[A-Za-z0-9_\.]+)) viber*')
    #re_find_skype1 = re.compile(r'.*скайп\:? ([A-Za-z0-9_\.]+(?:\s*[A-Za-z0-9_\.]+)) viber*')
    
    instr = [multdotrep(instr)]
    instr = [remove_punct_except(instr)]
    #instr = [remspace.sub('',instr)]

    
    regex_lst= [re_find_skype]
    if not sub:    
      for word in instr:
          for i in range(len(regex_lst)):
              res = regex_lst[i].findall(word)
              if res is not None:
                  joinstr=[', '.join([g]) for g in res if len(g)!=0]
              if len(joinstr)!=0:
                finalmatch.extend(joinstr)
      
          for i in range(len(finalmatch)):
      
              tokens = finalmatch[i]
              tres = tokens.strip().split()
              if len(tres) == 2:
                  joinstr=tres[0]+'.'+tres[1]
                  finalmatch[i] = joinstr
      
          #matchstr=', '.join(finalmatch)
          if len(finalmatch)!=0:
              return finalmatch
          else:
              return ['']    
    else:
      for word in instr:
          for i in range(len(regex_lst)):
              out = regex_lst[i].sub(' ',word)
              word = out  
      return word         

#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------
#def find_all_cells (instr, sub=False):
#
#   matchstr=''
#   finalmatch=[] 
#   phone_regex0 = re.compile(r'(0\d{9})') 
#   phone_regex1 = re.compile(r'(\d+\s*\d{3,}\s*\d+\s*\d+\s*\d+)')
#   
#   
#   pattern0 = '\+?[078]?\s*[-(\/]*?\s*\d{3}\s*[-)\/]*?\s*\d{3}\s*\-?\s*\d{2}\s*\-?\s*\d{2}'
#   pattern1 = '\+?[078]?\s*[-(\/]*?\s*\d{3}\s*[-)\/]*?\s*\d{2}\s*\-?\s*\d{2}\s*\-?\s*\d{3}'
#   pattern2 = '\+?[078]?\s*[-(\/]*?\s*\d{3}\s*[-)\/]*?\s*\d{2}\s*\-?\s*\d{3}\s*\-?\s*\d{2}'
#   pattern3 = '\+?[078]?\s*[-(\/]*?\s*\d{5}\s*[-)\/]*?\s*\d{1}\s*\-?\s*\d{2}\s*\-?\s*\d{2}'
#   pattern4 = '\+?[078]?\s*[-(\/]*?\s*\d{5}\s*[-)\/]*?\s*\d{2}\s*\-?\s*\d{3}'
#   #pattern5 = '\+?[078]?\s*[-(\/]*?\s*\d{3}\s*[-)\/]*?\s*\d{2}\s*\-?\s*\d{5}'
#   
#   pattern6 = '[(\/]?0\s*[-(\/]?\s*\d{2}\s*[-)\/]*?\s*\d{2}\s*\-?\s*\d{2}\s*\-?\s*\d{3}'
#   pattern7 = '[(\/]?\s*\d{3}\s*[-)\/]*?\s*\d{0,1}\s*\-?\s*\d{3}'
#   pattern8 = '[(\/]?\s*\d{2}\s*[-)\/]*?\s*\d{2}\s*\-?\s*\d{2}'
#   
#   phone_regex20 = re.compile(r'(?:\:?\,?\.?\s*)?(?<!id)(?<!icq)(?<![#\/])('+pattern0+')(?=(\s|,|)+)')
#   phone_regex21 = re.compile(r'(?:\:?\,?\.?\s*)?(?<!id)(?<!icq)(?<![#\/])('+pattern1+')(?=(\s|,|)+)')
#   phone_regex22 = re.compile(r'(?:\:?\,?\.?\s*)?(?<!id)(?<!icq)(?<![#\/])('+pattern2+')(?=(\s|,|)+)')
#   phone_regex23 = re.compile(r'(?:\:?\,?\.?\s*)?(?<!id)(?<!icq)(?<![#\/])('+pattern3+')(?=(\s|,|)+)')
#   phone_regex24 = re.compile(r'(?:\:?\,?\.?\s*)?(?<!id)(?<!icq)(?<![#\/])('+pattern4+')(?=(\s|,|)+)')
#   #phone_regex25 = re.compile(r'(?:\:?\,?\.?\s*)?(?<!id)(?<!icq)(?<![#\/])('+pattern5+')(?=(\s|,|)+)')
#   phone_regex26 = re.compile(r'(?:\:?\,?\.?\s*)?(?<!id)(?<!icq)(?<![#\/])(?<!\d[-(])('+pattern6+')(?=(\s|,|)+)')
#   phone_regex27 = re.compile(r'(?:\:?\,?\.?\s*)?(?<!id)(?<!icq)(?<![#\/])('+pattern7+')(?!=\d)(?=(\s|,|)+)')
#   phone_regex28 = re.compile(r'(?:\:?\,?\.?\s*)?(?<!id)(?<!icq)(?<![#\/])('+pattern8+')(?!=\d)(?=(\s|,|)+)')
#   
#   regex_lst=[phone_regex0, phone_regex20, phone_regex21, phone_regex22,phone_regex23,\
#              phone_regex24, phone_regex26, phone_regex27, phone_regex28]
#   
#   
#   instr = [find_url(instr,sub=True)]
#   
#   if not sub:       
#       for word in instr:
#         #word=word.strip()
#         for i in range(len(regex_lst)):
#           res=regex_lst[i].findall(word)
#           if len(res)!=0:
#               if isinstance(res,list):
#                   if isinstance(res[0],tuple):
#                       joinstr=[', '.join([g]) for ele in res for g in ele if len(g)!=0]
#                   else:
#                       joinstr=[', '.join([g]) for g in res if len(g)!=0]
#               else:
#                       joinstr=[res]
#
#                   
#               if len(joinstr)!=0:
#                 finalmatch.extend(joinstr) 
#             
#       #matchstr=', '.join(finalmatch)
#       if len(finalmatch)!=0:
#         return list(set(finalmatch))
#       else:
#         return ['']
#   else:
#     for word in instr:
#         for i in range(len(regex_lst)):
#             out = regex_lst[i].sub(' ',word)
#             word = out  
#     return word  

#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------    
#def find_cell(instr, sub=False):
#
#    finalmatch=[]
#
#    pattern0 = '\+?[078]?\s*[-(\/]*?\s*\d{3}\s*[-)\/]*?\s*\d{3}\s*\-?\s*\d{2}\s*\-?\s*\d{2}'
#    pattern1 = '\+?[078]?\s*[-(\/]*?\s*\d{3}\s*[-)\/]*?\s*\d{2}\s*\-?\s*\d{2}\s*\-?\s*\d{3}'
#    pattern2 = '\+?[078]?\s*[-(\/]*?\s*\d{3}\s*[-)\/]*?\s*\d{2}\s*\-?\s*\d{3}\s*\-?\s*\d{2}'
#    pattern3 = '\+?[078]?\s*[-(\/]*?\s*\d{5}\s*[-)\/]*?\s*\d{1}\s*\-?\s*\d{2}\s*\-?\s*\d{2}'
#    pattern4 = '\+?[078]?\s*[-(\/]*?\s*\d{5}\s*[-)\/]*?\s*\d{2}\s*\-?\s*\d{3}'    
#    
#    phone_regex0  = re.compile(r'(?:номеру|звоните|тел|связь|т)[\.: ]*((('+pattern0+')|('+pattern1+')|('+pattern2+')|('+pattern3+')|('+pattern4+'))'+'[\, ]*(('+pattern0+')|('+pattern1+')|('+pattern2+')|('+pattern3+')|('+pattern4+')))+', re.UNICODE)
#
#    
#    regex_lst=[phone_regex0]
#    
#    if not sub: 
#        for word in instr:
#          for i in range(len(regex_lst)):
#            res=regex_lst[i].findall(word)
#            if len(res)!=0:
#                if isinstance(res,list):
#                    if isinstance(res[0],tuple):
#                        joinstr=[', '.join([g]) for ele in res for g in ele if len(g)!=0]
#                    else:
#                        joinstr=[', '.join([g]) for g in res if len(g)!=0]
#                else:
#                    joinstr=[res]
#                    
#                if len(joinstr)!=0:
#                  finalmatch.extend(joinstr)    
#          
#        if len(finalmatch)!=0:
#          return list(set(finalmatch))
#        else:
#          return ['']     
#    else:
#      for word in instr:
#          for i in range(len(regex_lst)):
#              out = regex_lst[i].sub(' ',word)
#              word = out 
#      return out      
#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------      
 
def find_all_cells(instr, sub=False):

    finalmatch=[]
    remprice = re.compile(r'[1-9]+0{3,4}')
    remspace=re.compile(r'\s+')
    remplus =re.compile(r'^\+')
    #re_find_all_cell = re.compile(r'(\b\+?[78]?\d{10}\b)|(\b\d{6,7}?\b)|((?:[a-zа-я:.,-\—])\+?[78]?\d{10}(?:[a-zа-я:.,-\—]))|((?:[a-zа-я:.,-\— ])\d{6,7}(?:[a-zа-я:.,-\— ])(?!=\d)?)', re.UNICODE)
    
    re_find_all_cell = re.compile(r'(?:\b|\D)(\+?[78]?\d{10}|\d{6,7})(?:\b|\D)')
    
    instr = [find_url(instr,sub=True)]
    instr = [find_vkid(instr,sub=True)]
    instr = [find_skype(instr,sub=True)]
    instr = remove_punct_except(instr)
    instr = remprice.sub('',instr)
    instr = [remspace.sub('',instr)]
    
    #print(instr)
    
    if not sub: 
        for word in instr:
            res=re_find_all_cell.findall(word)
            if len(res)!=0:
                if isinstance(res,list):
                    if isinstance(res[0],tuple):
                        joinstr=[', '.join([remplus.sub('',g.strip())]) for ele in res for g in ele if len(g)!=0]
                    else:
                        joinstr=[', '.join([remplus.sub('',g.strip())]) for g in res if len(g)!=0]
                else:
                    joinstr=[res]
                    
                if len(joinstr)!=0:
                  finalmatch.extend(joinstr)    
          
        if len(finalmatch)!=0:
          return list(set(finalmatch))
        else:
          return ['']     
    else:
      for word in instr:
          for i in range(len(regex_lst)):
              out = regex_lst[i].sub(' ',word)
              word = out 
      return out 



#def find_chat(instr, sub=False):
#    finalmatch=[]
#    
#    pattern = '\+?[078]?\s*[\-\(]?\s*\d{3}\s*[\-\)]?\s*\d{3}\s*\-?\s*\d{2}\s*\-?\s*\d{1,2}'
#    re_find_chat  = re.compile(r'(?:viber|вайбер|вотсап|ватсап|whatsup|telegram|телеграм)[:\s+]*'+'('+pattern+'(?:\,?\s*'+pattern+')*)', #re.UNICODE)
#
#    if not sub:     
#        for word in instr:
#          res=re_find_chat.findall(word)
#          if res is not None:
#            joinstr=[', '.join([g]) for g in res if len(g)!=0]
#            if len(joinstr)!=0:
#              finalmatch.extend(joinstr)    
#          
#        if len(finalmatch)!=0:
#          return finalmatch
#        else:
#          return ['']     
#    else:
#      for word in instr:
#        out = re_find_chat.sub(' ',word)
#      return out  

#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------
def find_vkid(instr, sub=False):
    finalmatch=[]
    #vkid_regex=re.compile(r'(?:id(?<=id)\d+)?(?:vk.com\/(?!video|wall)[А-Яа-яA-Za-z0-9_\.]+)?')
    vkid_regex=re.compile(r'(vk.com/(?<=vk.com/)(?!video)([a-z0-9_.]+|id\d+))| \
                            (vkontakte.ru/(?<=vkontakte.ru/)(?!video)([a-z0-9_.]+|id\d+))|(id\d+)')
    
    instr = [multdotrep(instr)]
   
    if not sub: 
        for word in instr:
          res=vkid_regex.findall(word)
          if len(res)!=0:
                if isinstance(res,list):
                    if isinstance(res[0],tuple):
                        joinstr=[', '.join([g]) for ele in res for g in ele if len(g)!=0]
                    else:
                        joinstr=[', '.join([g]) for g in res if len(g)!=0]
                else:
                    joinstr=[res]
                if len(joinstr)!=0:
                  finalmatch.extend(joinstr)    
                    
        if len(finalmatch)!=0:
          return finalmatch
        else:
          return ['']
    else:
      for word in instr:
        out = vkid_regex.sub('',word)
      return out 


def find_chat(instr, sub=False):
    
    finalmatch=[]
    remplus =re.compile(r'^\+')
    remspace=re.compile(r'\s+')
    
    re_find_chat = re.compile(r'(?:viber|вайбер|ваибер|вибер|вотсап|ватсап|whatsup|whatsupp|telegram|телеграм|т|тел|телефону|телефон|звоните|связь)(?:[:.,]*)(\+?[78]?\d{10}|\d{6,7})(?:[:.,]*)(\+?[78]?\d{10}|\d{6,7})?', re.UNICODE)

    
    instr = [multdotrep(instr)]
    instr = remove_punct_except(instr)
    instr = [remspace.sub('',instr)] 
    
    if not sub:     
        for word in instr:
          res=re_find_chat.findall(word)
          if len(res)!=0:
                if isinstance(res,list):
                    if isinstance(res[0],tuple):
                        joinstr=[', '.join([remplus.sub('',g.strip())]) for ele in res for g in ele if len(g)!=0]
                    else:
                        joinstr=[', '.join([remplus.sub('',g.strip())]) for g in res if len(g)!=0]
                else:
                    joinstr=[remplus.sub('',res)]
                    
                if len(joinstr)!=0:
                  finalmatch.extend(joinstr)   
          
        if len(finalmatch)!=0:
          return finalmatch
        else:
          return ['']     
    else:
      for word in instr:
        out = re_find_chat.sub(' ',word)
      return out  


#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------
def multdotrep(instr, sub=None):
    re_multdotrep = re.compile(r'\.{2,}')
    for word in instr:  
      out = re_multdotrep.sub(',', word)
    return out

#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------
def remove_digits(instr, sub=None):
    digitrem = re.compile(r'\d+')
    for word in instr:
      out = digitrem.sub(' ', word)
    return out

def remove_eng_leters(instr, sub=None):
    rem_eng = re.compile(r'[A-Za-z]+')
    for word in instr:
      out = rem_eng.sub('', word)
    return out


#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------
def remove_short_wds(instr, sub=None):
    for word in instr:
        out = ' '.join(ele for ele in word.split() if len(ele)>=2).lower()
    return out



#----------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------
def remove_punctuation(instr, sub=None):
    exclude = set(string.punctuation)
    for word in instr:
        punc_free = ''.join(ch if ch not in exclude else ' ' for ch in word)
    return punc_free

def remove_punct_except(instr, sub=None):
    exclude = set(string.punctuation)
    exclude = exclude.difference(set([',',';',':','.']))
    for word in instr:
        punc_free = ''.join(ch if ch not in exclude else ' ' for ch in word)
    return punc_free



preprocessing_setps = OrderedDict(
                                   [
                                    ('multdotrep'        , multdotrep),
                                    ('find_all_cells'    , find_all_cells),
                                    #('find_cell'         , find_cell),
                                    ('find_chat'         , find_chat),
                                    ('find_email'        , find_email),
                                    ('find_vkid'         , find_vkid), 
                                    ('find_url'          , find_url),
                                    ('find_skype'        , find_skype),
                                    ('find_instagram'    , find_instagram),
                                    ('remove_digits'     , remove_digits),
                                    ('remove_punctuation', remove_punctuation),
                                    ('split_on_uppercase', split_on_uppercase),
                                    ('remove_eng_leters' , remove_eng_leters),   
                                    ('remove_short_wds'  , remove_short_wds)   
                                   ]
                                 ) 
 

#****************************************************************
#****************************************************************
def process_text(steps,text, sub=False):
    if steps is not None:
        for step in steps:
          text = preprocessing_setps[step]([text], sub)
    return text       
#****************************************************************
#****************************************************************





