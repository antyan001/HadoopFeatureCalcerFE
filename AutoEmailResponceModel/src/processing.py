import os
import sys
import warnings
warnings.filterwarnings('ignore')

from pathlib import Path
import re 
import joblib
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

from multiprocessing import Pool, Process, JoinableQueue
from multiprocessing.pool import ThreadPool
import numpy as np
import pandas as pd
from tqdm import tqdm

import pyspark
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame


class ClickRateCalc(object):

    def __init__(self):
        self.dummy = None
    
    def get_info(self, df):
        print('start_info \n', df.shape)
        
        #*************************************************************************************************************
        def get_delivery_rate(triggered_count_series, delivery_count_series):
            
            delivery_rate_series = (triggered_count_series / delivery_count_series) * 100  # rate of success messages
            delivery_rate_series.replace(np.inf, 0, inplace=True)   # np.inf shows if deliverycnt is 0
            
            return delivery_rate_series
        #*************************************************************************************************************
        def check_trig(x, months: int) -> int:

            response_dttm = x['response_dttm']  # just some response datetime
            last_max_day = x['last_max_day']  # datetime of last communication with client (sent message)

            first_day = last_max_day - relativedelta(months=months)  # 1st day of window to check response into (window size in months)

            month_days = 31
            return 1 if np.abs((response_dttm - first_day).days) <= month_days * months else 0  # check if response is in window
        #*************************************************************************************************************    
        def do_with_df(gr_df):
 
            gr_df['trg_m2'] = gr_df.apply(lambda x: check_trig(x, 2), axis=1)  # check if x.response if in 2 months
            gr_df['trg_m4'] = gr_df.apply(lambda x: check_trig(x, 4), axis=1)  # check if x.response if in 4 months
            gr_df['trg_m6'] = gr_df.apply(lambda x: check_trig(x, 6), axis=1)  # check if x.response if in 6 months

            # count "page open" status in a day history
            for i in range(0, 6, 2):
                mon = str(i+2)
                gr_df['maxday_trg_m'+mon] = gr_df.groupby(['inn', 'campaign_nm', 'response', 'asdayname', 'trg_m'+mon])['inn'].transform('count')
                
                # gr_df['open_click_day_rate_m'+mon] = (gr_df['maxday_trg_m'+mon] / gr_df['deliverycnt'])*100  # rate of success messages
                # gr_df['open_click_day_rate_m'+mon].replace(np.inf, 0, inplace=True)   # np.inf shows if deliverycnt is 0
                
                gr_df['open_click_day_rate_m'+mon] = get_delivery_rate(gr_df['maxday_trg_m'+mon], gr_df['deliverycnt'])
                
            return gr_df
        #************************************************************************************************************* 
        
        # 2018-09-14 17:33:53
        # 2018-09-14
        print('day convert \n', df.shape)
        df['asday'] =   df['response_dttm'].apply(lambda x: datetime.strftime(x, format='%Y-%m-%d'))
        # 17-33
        df['astime'] =   df['response_dttm'].apply(lambda x: datetime.strftime(x, format='%H:%M'))
        # 17
        df['ashour'] =  df['response_dttm'].apply(lambda x: datetime.strftime(x, format='%H'))
        # Fri
        df['asdayname'] =  df['response_dttm'].apply(lambda x: datetime.strftime(x, format='%a'))

        print('count open page \n', df.shape)
        # count "open page" status in a day
        df['maxday']  = df.groupby(['inn', 'campaign_nm', 'response', 'asdayname'])['inn'].transform('count')
        # count "open page" status in a hour
        df['maxhour']  = df.groupby(['inn', 'campaign_nm', 'response', 'asdayname', 'ashour'])['inn'].transform('count')

        # first date when sent email
        df.sort_values(['inn', 'campaign_nm', 'response_dttm'], inplace= True)
        df['delivery'] = df['response'] == 'SendSay. Сообщение доставлено'  # Bool if messange is delivered
        df['last_send_time'] = df['delivery'].cumsum()
        df['last_send_time'] = df.groupby(['inn', 'campaign_nm', 'last_send_time'])['response_dttm'].transform('first')

        print('interval \n', df.shape)
        # interval of response time per max day
        df['interv_day'] = df.groupby(['inn', 'campaign_nm', 'response', 'maxday'])['astime'].transform(lambda x: str(x.min()) +' - '+ str(x.max()))
        # interval of response time per max hour
        df['interv_hour'] = df.groupby(['inn', 'campaign_nm', 'response', 'maxday', 'ashour'])['astime'].transform(lambda x: str(x.min()) +' - '+ str(x.max()))
        # interval of response to last delivery
        df['time_after_last_delivery'] = df['response_dttm'] - df['last_send_time'] 

        print('delivery info \n', df.shape)
        # count delivered emails for each user
        df['deliverycnt'] = df.groupby(['inn', 'campaign_nm'])['delivery'].transform('sum')
        # rate of success delivered messages by user
        # df['open_click_day_rate'] =(df['maxday'] / df['deliverycnt']) * 100
        # df['open_click_day_rate'].replace(np.inf, 0, inplace=True)  # if deliverycnt is 0 then np.inf shows
        df['open_click_day_rate'] = get_delivery_rate(df['maxday'], df['deliverycnt'])
        
        # last max day for history
        df['last_max_day']  = df.groupby(['inn', 'campaign_nm', 'response'])['response_dttm'].transform('max')

        print('histor \n', df.shape)
        df = do_with_df(gr_df= df)

        return df


class Preproc_Response(ClickRateCalc):
    
    def __init__(self):
        super(Preproc_Response, self).__init__()

    def show(self, n=10):
        return self.limit(n).toPandas()

    def get_inn_batch(self, spdf, ctrow=None):
        if ctrow is not None:
            maxit = ctrow
        else:
            maxit = spdf.count()
        print('Load batch', datetime.now().strftime(format='%Y-%m-%d %H:%M'))
        spdf_part = spdf.show(maxit)
        batches = []
        for item in range(spdf_part.batch.max()+1):
            batches.append(tuple(spdf_part[spdf_part['batch'] == item].inn.tolist()))
        return batches    

    
    def split_df(self, df_all, df_grp, ctrow=None):
        print('Load all', datetime.now().strftime(format='%Y-%m-%d %H:%M'))
        # load full dataframe from spark
        if ctrow is None:
            ctrow = df_all.count()    
        print(ctrow)
        df_full = df_all.show(ctrow)
        # load grp
        batches = self.get_inn_batch(df_grp, ctrow)
        for batch in batches:
            collection = df_full[df_full['inn'].isin(batch)]
            print('search  ', collection.shape, datetime.now().strftime(format='%Y-%m-%d %H:%M'))
            yield collection
        #return collection
      
    
    def analyse(self, df, batch, func=None):
        if func is None:
            func = self.get_info
        worker_args = [__iter for __iter in self.split_df(df_all=df, df_grp=batch)]
        poollen = len(worker_args)
        print('tread ',poollen)
        ibatch = np.array_split(range(poollen), 4)
        for tr in tqdm(ibatch):
            user_pool = ThreadPool(len(ibatch))
            #user_pool = Pool(len(ibatch))
            res_df = pd.concat(user_pool.map(func, worker_args[tr[0]:tr[-1]+1]))
            user_pool.close()
            user_pool.join()

        return res_df
    
    def split_df_2(self, sdf, ctrow=None):
        print('Load all', datetime.now().strftime(format='%Y-%m-%d %H:%M'))
        # load full dataframe from spark   
        batches = range(sdf.agg({'batch': 'max'}).collect()[0]['max(batch)']+1)

        for batch in batches:
            while True:
                try:
                    collection = sdf.filter('batch = {}'.format(batch))
                    cnt = collection.count()
                    res = collection.show(cnt)
                    print(res.shape)
                    break
                except Exception as ex:
                    print('soket error')
            
            yield res
    