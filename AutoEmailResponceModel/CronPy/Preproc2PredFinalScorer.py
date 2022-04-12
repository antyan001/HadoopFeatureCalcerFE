import os
import sys
import warnings
warnings.filterwarnings('ignore')

from collections import OrderedDict
from pathlib import Path
import pandas as pd 
import numpy as np
import re 
import joblib

pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

curruser = 'ektov1-av_ca-sbrf-ru'
curruser_main = os.environ.get('USER')

isUseOptWorkspace = False
sys.path.insert(0, './../src')

if isUseOptWorkspace:
    sys.path.insert(0, '/opt/workspace/{}/notebooks/ecom_model/src/'.format(curruser))
    sys.path.insert(0, '/opt/workspace/{}/notebooks/support_library/'.format(curruser)) 
    sys.path.insert(0, '/opt/workspace/{}/libs/python3.5/site-packages/'.format(curruser))
    sys.path.insert(0, '/opt/workspace/{}/notebooks/labdata/lib/'.format(curruser_main))
else:
    sys.path.insert(0, '/home/{}/notebooks/ecom_model/src/'.format(curruser))
    sys.path.insert(0, '/home/{}/notebooks/support_library/'.format(curruser)) 
    sys.path.insert(0, '/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
    sys.path.insert(0, '/home/{}/notebooks/labdata/lib/'.format(curruser_main))

from preprosessing import Preproc
from corpora_transform_main import TfIdfFastTextEmbedVectorizer

# from tqdm import tqdm
# from tqdm._tqdm_notebook import tqdm_notebook
# tqdm_notebook.pandas()

from spark_connector import SparkConnector
from sparkdb_loader import spark
# from spark_helper import SparkHelper
from connector import OracleDB
import pyspark
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer

# import loader as load

# from corpora_process import utils
from processing import Preproc_Response
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

from itertools import islice
from multiprocessing import Pool, Process, JoinableQueue
from multiprocessing.pool import ThreadPool
from functools import partial
import numpy as np
import subprocess
from threading import Thread
from joblib import Parallel, delayed


def drop_col(df, cols: list):
    scol = sdf.columns
    final_cols = [i for i in scol if i not in cols]
    return df.select(*final_cols)

def show(self, n=10):
    return self.limit(n).toPandas()
pyspark.sql.dataframe.DataFrame.show = show


table_name = 'sendsay_data_predict'
sp = spark(schema='sbx_team_digitcamp', 
           sparkVersion='2.2',
           dynamic_alloc=False,
           numofinstances=20, 
           numofcores=8,
           kerberos_auth=False,
           output=table_name) 
print(sp.sc.version)
hive = sp.sql


# Collect all user responses for the whole historical period
all_sdf = hive.sql('''
with
--v0.0.7
--04.12.2019 19:20
w_resp as(
      select 
        regexp_extract(organization_id, '(\\\\d{10,12})(?:_)') as inn,
        regexp_extract(organization_id, '(?:_)(\\\\d{3,})')    as kpp,
        organization_id,
        concat_ws('_', organization_id, resp_tracking_cd) as custid_value,
        campaign_nm,
        response_dttm,
        response,
        load_dt
        
       from sbx_team_digitcamp.sme_cdm_v_response
      where lower(response) rlike '^(?=.*sendsay)(?!.*(ошиб|отпис|спам|не удалось|в процессе|доставлено)).*' 
        --and load_dt <= timestamp('%s')
        --and organization_id like '%6446004324%'  
        --and organization_id like '%6454010956%'
        
),
w_send as (
    select 
      regexp_extract(organization_id, '(\\\\d{10,12})(?:_)') as inn,
      regexp_extract(organization_id, '(?:_)(\\\\d{3,})') as kpp,
      organization_id,
      tracking_id as custid_value,
      campaign_nm,
      int_update_dttm as response_dttm,
      'SendSay. Сообщение доставлено' as response,
      load_dt,
      email, email_subject,
      customer_text_01, customer_text_02, customer_text_03, customer_text_04, customer_text_05,
      customer_text_06, customer_text_07, customer_text_08, customer_text_09, customer_text_10,
      customer_text_11, customer_text_12, customer_text_13, customer_text_14, customer_text_15,
      customer_text_16, customer_text_17, customer_text_18, customer_text_19, customer_text_20,
      message_title
    from sbx_team_digitcamp.sme_email_to_sendsay
   where 1=1
     --and load_dt <= timestamp('%s')
     --and organization_id like '%6446004324%'  
     --and organization_id like '%6454010956%'
),
w_only_send as (
    select s.* from (select distinct organization_id from w_resp) r
        left join w_send s on r.organization_id = s.organization_id 
),
w_base as(
    select * from 
    (--text columns in line for response
        select 
         row_number() over (partition by r.organization_id, r.campaign_nm, r.response_dttm, r.response order by r.response_dttm) as rn,
         r.inn, r.kpp, r.organization_id, r.custid_value, r.campaign_nm, r.response_dttm, r.response,
         r.load_dt,
         s.email, s.email_subject,
         s.customer_text_01, s.customer_text_02, s.customer_text_03, s.customer_text_04, s.customer_text_05,
         s.customer_text_06, s.customer_text_07, s.customer_text_08, s.customer_text_09, s.customer_text_10,
         s.customer_text_11, s.customer_text_12, s.customer_text_13, s.customer_text_14, s.customer_text_15,
         s.customer_text_16, s.customer_text_17, s.customer_text_18, s.customer_text_19, s.customer_text_20,
         s.message_title
        from w_resp r
        left join w_only_send s on r.organization_id = s.organization_id
                              and r.campaign_nm = s.campaign_nm                 
         where length(r.inn) >=10
        ) where rn = 1
    
    union

    select '' as rn, * from w_only_send
     where length(inn) >=10
)
select 
       inn
      ,kpp 
      ,campaign_nm 
      ,organization_id
      ,custid_value
      ,response_dttm
      ,response
      ,date_format(response_dttm, 'yyyy-MM-dd') as asday
      ,date_format(last_day(response_dttm), 'yyyy-MM-dd') as last_day
      ,date_format(response_dttm, 'HH:mm:ss') as astime
      ,date_format(response_dttm, 'HH') as ashour
      ,date_format(response_dttm, 'EEEE') as asdayname
      ,load_dt
      ,email
      ,email_subject,
      customer_text_01, customer_text_02, customer_text_03, customer_text_04, customer_text_05,
      customer_text_06, customer_text_07, customer_text_08, customer_text_09, customer_text_10,
      customer_text_11, customer_text_12, customer_text_13, customer_text_14, customer_text_15,
      customer_text_16, customer_text_17, customer_text_18, customer_text_19, customer_text_20,
      message_title    
from w_base   
''') #% (max_load_dt_str, max_load_dt_str))

# Collect a list of daytime values based on the distinct values of last_day column
last_day_rows = all_sdf.select('last_day').distinct().collect()
last_day_lst = [day['last_day'] for day in last_day_rows if day['last_day'] is not None]
last_day_dt = sorted([datetime.strptime(day_str, '%Y-%m-%d') for day_str in last_day_lst])

# SAMPLING FOR VALIDATION DATASET
val_sdf = all_sdf.filter((f.col('response_dttm') >= last_day_dt[-4]))

# Find min value of response_dttm in VAL dataset
resp_dttm_min = val_sdf.select(f.min('response_dttm')).collect()
resp_dttm_min_str = resp_dttm_min[0]['min(response_dttm)']

# Roll back by 3 month from the min value of last_open_time and generate a temp dataframe for feature generation process
sendsay_delivered = 'SendSay. Сообщение доставлено'
sendsay_openpage  = 'SendSay. Открытие страницы'
sendsay_openlink  = 'SendSay. Переход по ссылке' 

val_sdf = val_sdf \
        .withColumn('first_send_time', 
                    f.min(f.when(f.col('response') == sendsay_delivered, f.col('response_dttm')).otherwise(None))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))\
        .withColumn('last_send_time', 
                    f.max(f.when(f.col('response') == sendsay_delivered, f.col('response_dttm')).otherwise(None))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))\
        .withColumn('first_open_time', 
                    f.min(f.when((f.col('response') == sendsay_openpage), f.col('response_dttm')).otherwise(None))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))\
        .withColumn('last_open_time', 
                    f.max(f.when((f.col('response') == sendsay_openpage), f.col('response_dttm')).otherwise(None))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))\
        .withColumn('first_click_time', 
                    f.min(f.when((f.col('response') == sendsay_openlink), f.col('response_dttm')).otherwise(None))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))\
        .withColumn('last_click_time', 
                    f.max(f.when((f.col('response') == sendsay_openlink),f.col('response_dttm')).otherwise(None))\
                                      .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))
shiftd = 90
shift_back  = val_sdf.select(f.last_day(f.add_months(f.trunc(f.from_unixtime(f.unix_timestamp(f.min('last_open_time')) - shiftd*24*60*60),'MM'),-1)).alias('monthback')).collect()
shift_back  = shift_back[0]['monthback']

sdf = all_sdf.filter((f.col('response_dttm') >= shift_back))
del val_sdf

# Main Features Generation Pipeline
# Find inactive users (users without any open/click activity among overall history of communication)

sdf = sdf \
        .withColumn('first_send_time', 
                    f.min(f.when(f.col('response') == sendsay_delivered, f.col('response_dttm')).otherwise(None))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))\
        .withColumn('last_send_time', 
                    f.max(f.when(f.col('response') == sendsay_delivered, f.col('response_dttm')).otherwise(None))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))\
        .withColumn('first_open_time', 
                    f.min(f.when((f.col('response') == sendsay_openpage), f.col('response_dttm')).otherwise(None))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))\
        .withColumn('last_open_time', 
                    f.max(f.when((f.col('response') == sendsay_openpage), f.col('response_dttm')).otherwise(None))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))\
        .withColumn('first_click_time', 
                    f.min(f.when((f.col('response') == sendsay_openlink), f.col('response_dttm')).otherwise(None))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))\
        .withColumn('last_click_time', 
                    f.max(f.when((f.col('response') == sendsay_openlink),f.col('response_dttm')).otherwise(None))\
                                      .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')))
inactive_freshhold_min = 2
inactive_freshhold_max = 20

sdf = sdf.withColumn('sum_open_click', 
                     f.sum(f.when((f.col('response') == sendsay_openpage)|\
                                  (f.col('response') == sendsay_openlink),1).otherwise(0))\
                                                           .over(Window.partitionBy('inn', 'kpp', 'email')))

sdf = sdf.withColumn('sum_open_click_camp', 
                     f.sum(f.when((f.col('response') == sendsay_openpage)|\
                                  (f.col('response') == sendsay_openlink),1).otherwise(0))\
                                                           .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')))

sdf = sdf.withColumn('delivery_all_cnt', 
                                         f.sum((f.col('response') == sendsay_delivered).cast('integer')) \
                                          .over(Window.partitionBy('inn', 'kpp', 'email')).cast('integer'))

sdf = sdf.withColumn('fresh_user', f.when((f.col('sum_open_click') == 0) & 
                                          (f.col('delivery_all_cnt') <= inactive_freshhold_min), 1)\
                                    .when((f.col('sum_open_click') == 0) & 
                                          (f.col('delivery_all_cnt') > inactive_freshhold_min), 2).otherwise(0))

sdf = sdf.drop(*['delivery_all_cnt','sum_open_click'])


# Retrospective Features Generation Pipeline

daysback = 30

for i in range(1,4):
    
    dttm_left = f.from_unixtime(f.unix_timestamp(f.col('last_send_time')) - daysback*(i)*24*60*60, 
                                'yyyy-MM-dd HH:mm:ss').cast(TimestampType())

    dttm_right = f.from_unixtime(f.unix_timestamp(f.col('last_send_time')) - daysback*(i-1)*24*60*60, 
                                 'yyyy-MM-dd HH:mm:ss').cast(TimestampType())    
    
    sdf = sdf \
            .withColumn('first_send_time_m{}1'.format(str(i)), 
                        f.min(f.when((f.col('response') == sendsay_delivered) &\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), f.col('response_dttm'))\
                                    .otherwise(None))\
                        .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')))\
            .withColumn('last_send_time_m{}1'.format(str(i)), 
                        f.max(f.when((f.col('response') == sendsay_delivered) &\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), f.col('response_dttm'))\
                                    .otherwise(None))\
                        .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')))

for i in range(1,4):            

    dttm_left = f.from_unixtime(f.unix_timestamp(f.col('last_open_time')) - daysback*(i)*24*60*60, 
                                'yyyy-MM-dd HH:mm:ss').cast(TimestampType())

    dttm_right = f.from_unixtime(f.unix_timestamp(f.col('last_open_time')) - daysback*(i-1)*24*60*60, 
                                 'yyyy-MM-dd HH:mm:ss').cast(TimestampType())

    sdf = sdf \
            .withColumn('first_open_time_m{}1'.format(str(i)), 
                        f.min(f.when((f.col('response') == sendsay_openpage) &\
                                     (f.col('fresh_user')==0)&\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), f.col('response_dttm'))\
                                    .otherwise(None))\
                        .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')))\
            .withColumn('last_open_time_m{}1'.format(str(i)), 
                        f.max(f.when((f.col('response') == sendsay_openpage) &\
                                     (f.col('fresh_user')==0)&\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), f.col('response_dttm'))\
                                    .otherwise(None))\
                        .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')))
            
for i in range(1,4):
    
    dttm_left = f.from_unixtime(f.unix_timestamp(f.col('last_click_time')) - daysback*(i)*24*60*60,
                                'yyyy-MM-dd HH:mm:ss').cast(TimestampType())

    dttm_right = f.from_unixtime(f.unix_timestamp(f.col('last_click_time')) - daysback*(i-1)*24*60*60,
                                 'yyyy-MM-dd HH:mm:ss').cast(TimestampType())
    
    sdf = sdf \
            .withColumn('first_click_time_m{}1'.format(str(i)), 
                        f.min(f.when((f.col('response') == sendsay_openlink) &\
                                     (f.col('fresh_user')==0)&\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), f.col('response_dttm'))\
                                    .otherwise(None))\
                        .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')))\
            .withColumn('last_click_time_m{}1'.format(str(i)), 
                        f.max(f.when((f.col('response') == sendsay_openlink) &\
                                     (f.col('fresh_user')==0)&\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), f.col('response_dttm'))\
                                    .otherwise(None))\
                        .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')))
            
            
# Calculate monthly biased time events with status == sendsay_openpage   

for i in range(1,4):
    
    dttm_left = f.from_unixtime(f.unix_timestamp(f.col('last_open_time')) - daysback*(i)*24*60*60, 
                                'yyyy-MM-dd HH:mm:ss').cast(TimestampType())

    dttm_right = f.from_unixtime(f.unix_timestamp(f.col('last_open_time')) - daysback*(i-1)*24*60*60, 
                                 'yyyy-MM-dd HH:mm:ss').cast(TimestampType())     
    sdf = sdf \
            .withColumn('last_open_all_cnt_m{}1'.format(str(i)), 
                        f.sum(f.when((f.col('response') == sendsay_openpage) &\
                                     (f.col('fresh_user')==0)&\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), 1)\
                                                       .otherwise(0))\
                                                 .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')).cast('integer'))
        
for i in range(1,4):
    
    dttm_left = f.from_unixtime(f.unix_timestamp(f.col('last_open_time')) - daysback*(i)*24*60*60, 
                                'yyyy-MM-dd HH:mm:ss').cast(TimestampType())

    dttm_right = f.from_unixtime(f.unix_timestamp(f.col('last_open_time')) - daysback*(i-1)*24*60*60, 
                                 'yyyy-MM-dd HH:mm:ss').cast(TimestampType())     
    sdf = sdf \
            .withColumn('last_open_day_cnt_m{}1'.format(str(i)), 
                        f.sum(f.when((f.col('response') == sendsay_openpage) &\
                                     (f.col('fresh_user')==0)&\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), 1)\
                                                       .otherwise(0))\
                                                 .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 
                                                                          'email', 'asdayname')).cast('integer'))
        

# Calculate monthly biased time events with status == sendsay_openlink        
for i in range(1,4):
    
    dttm_left = f.from_unixtime(f.unix_timestamp(f.col('last_click_time')) - daysback*(i)*24*60*60, 
                                'yyyy-MM-dd HH:mm:ss').cast(TimestampType())

    dttm_right = f.from_unixtime(f.unix_timestamp(f.col('last_click_time')) - daysback*(i-1)*24*60*60, 
                                 'yyyy-MM-dd HH:mm:ss').cast(TimestampType())    
    
    sdf = sdf \
            .withColumn('last_click_all_cnt_m{}1'.format(str(i)), 
                        f.sum(f.when((f.col('response') == sendsay_openlink) &\
                                     (f.col('fresh_user')==0)&\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), 1)\
                                                       .otherwise(0))\
                                                 .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')).cast('integer'))
        
for i in range(1,4):
    
    dttm_left = f.from_unixtime(f.unix_timestamp(f.col('last_click_time')) - daysback*(i)*24*60*60, 
                                'yyyy-MM-dd HH:mm:ss').cast(TimestampType())

    dttm_right = f.from_unixtime(f.unix_timestamp(f.col('last_click_time')) - daysback*(i-1)*24*60*60, 
                                 'yyyy-MM-dd HH:mm:ss').cast(TimestampType())  
    
    sdf = sdf \
            .withColumn('last_click_day_cnt_m{}1'.format(str(i)), 
                        f.sum(f.when((f.col('response') == sendsay_openlink) &\
                                     (f.col('fresh_user')==0)&\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), 1)\
                                                       .otherwise(0))\
                                                 .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 
                                                                          'email', 'asdayname')).cast('integer'))
        
# Calculate time differences between events with status == sendsay_openpage        
for i in range(1,4):
    sdf = sdf \
            .withColumn('diff_lo_fo_m{}1'.format(str(i)), ((f.unix_timestamp('last_open_time_m{}1'.format(str(i))) -
                                                                   f.unix_timestamp('first_open_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_lo_fs_m{}1'.format(str(i)), ((f.unix_timestamp('last_open_time_m{}1'.format(str(i))) -
                                                                   f.unix_timestamp('first_send_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_lo_ls_m{}1'.format(str(i)), ((f.unix_timestamp('last_open_time_m{}1'.format(str(i))) -
                                                                   f.unix_timestamp('last_send_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_fo_fs_m{}1'.format(str(i)), ((f.unix_timestamp('first_open_time_m{}1'.format(str(i))) -
                                                                   f.unix_timestamp('first_send_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_fo_ls_m{}1'.format(str(i)), ((f.unix_timestamp('first_open_time_m{}1'.format(str(i))) -
                                                                   f.unix_timestamp('last_send_time_m{}1'.format(str(i))))/60)
                       )   
            
# Calculate time differences between events with status == sendsay_openlink            
for i in range(1,4):
    sdf = sdf \
            .withColumn('diff_lcl_fcl_m{}1'.format(str(i)), ((f.unix_timestamp('last_click_time_m{}1'.format(str(i))) -
                                                                     f.unix_timestamp('first_click_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_lcl_fs_m{}1'.format(str(i)), ((f.unix_timestamp('last_click_time_m{}1'.format(str(i))) -
                                                                    f.unix_timestamp('first_send_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_lcl_ls_m{}1'.format(str(i)), ((f.unix_timestamp('last_click_time_m{}1'.format(str(i))) -
                                                                    f.unix_timestamp('last_send_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_lcl_fo_m{}1'.format(str(i)), ((f.unix_timestamp('last_click_time_m{}1'.format(str(i))) -
                                                                    f.unix_timestamp('first_open_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_lcl_lo_m{}1'.format(str(i)), ((f.unix_timestamp('last_click_time_m{}1'.format(str(i))) -
                                                                    f.unix_timestamp('last_open_time_m{}1'.format(str(i))))/60)
                       )   


    sdf = sdf \
            .withColumn('diff_fcl_fs_m{}1'.format(str(i)), ((f.unix_timestamp('first_click_time_m{}1'.format(str(i))) -
                                                                    f.unix_timestamp('first_send_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_fcl_ls_m{}1'.format(str(i)), ((f.unix_timestamp('first_click_time_m{}1'.format(str(i))) -
                                                                    f.unix_timestamp('last_send_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_fcl_fo_m{}1'.format(str(i)), ((f.unix_timestamp('first_click_time_m{}1'.format(str(i))) -
                                                                    f.unix_timestamp('first_open_time_m{}1'.format(str(i))))/60)
                       )\
            .withColumn('diff_fcl_lo_m{}1'.format(str(i)), ((f.unix_timestamp('first_click_time_m{}1'.format(str(i))) -
                                                                    f.unix_timestamp('last_open_time_m{}1'.format(str(i))))/60)
                       )     
            
            
# Calculate events regarding to status == sendsay_delivered            
for i in range(1,4):
    
    dttm_left = f.from_unixtime(f.unix_timestamp(f.col('last_open_time')) - daysback*(i)*24*60*60, 
                                'yyyy-MM-dd HH:mm:ss').cast(TimestampType())

    dttm_right = f.from_unixtime(f.unix_timestamp(f.col('last_open_time')) - daysback*(i-1)*24*60*60, 
                                 'yyyy-MM-dd HH:mm:ss').cast(TimestampType())     
    
    sdf = sdf \
            .withColumn('delivery_all_cnt_m{}1'.format(str(i)), 
                        f.sum(f.when((f.col('response') == sendsay_delivered) &\
                                     ((f.col('response_dttm')> dttm_left) &\
                                      (f.col('response_dttm')<=dttm_right)), 1)\
                                                       .otherwise(0))\
                                                 .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')).cast('integer'))
        
        
# Calculate overall/monthly open_day_rate/click_day_rate        
def get_delivery_rate(triggered_count: int, delivery_count: int):
    """
    Get the rate of success messages.
    """
    if delivery_count == 0:
        return 0.0
    
    delivery_rate = (triggered_count / delivery_count) * 100  # rate of success messages
    return delivery_rate


get_delivery_rate_udf = f.udf(get_delivery_rate, FloatType())

for i in [str(ii) for ii in range(1,4)]:
#     sdf = sdf.withColumn('open_day_rate_m'+i, get_delivery_rate_udf("last_open_time_sum_m"+i, "deliverycnt"))
    sdf = sdf.withColumn('open_on_dlv_all_rate_m'+i+'1', 
                         get_delivery_rate_udf("last_open_all_cnt_m"+i+'1', "delivery_all_cnt_m"+i+'1'))
#     sdf = sdf.withColumn('open_on_dlv_all_dist_rate_m'+i+'1', 
#                          get_delivery_rate_udf("last_open_all_cnt_dist_m"+i+'1', "delivery_all_cnt_m"+i+'1'))
#     sdf = sdf.withColumn('click_day_rate_m'+i, get_delivery_rate_udf("last_click_time_sum_m"+i, "deliverycnt"))
    sdf = sdf.withColumn('clk_on_dlv_all_rate_m'+i+'1', 
                         get_delivery_rate_udf("last_click_all_cnt_m"+i+'1', "delivery_all_cnt_m"+i+'1'))
#     sdf = sdf.withColumn('clk_on_dlv_all_dist_rate_m'+i+'1', 
#                          get_delivery_rate_udf("last_click_all_cnt_dist_m"+i+'1', "delivery_all_cnt_m"+i+'1'))    
    sdf = sdf.withColumn('clk_on_open_all_rate_m'+i+'1', 
                         get_delivery_rate_udf("last_click_all_cnt_m"+i+'1', "last_open_all_cnt_m"+i+'1'))
#     sdf = sdf.withColumn('clk_on_open_all_dist_rate_m'+i+'1', 
#                          get_delivery_rate_udf("last_click_all_cnt_dist_m"+i+'1', "last_open_all_cnt_dist_m"+i+'1'))     

for i in [str(ii) for ii in range(1,4)]:
#     sdf = sdf.withColumn('open_day_rate_m'+i, get_delivery_rate_udf("last_open_time_sum_m"+i, "deliverycnt"))
    sdf = sdf.withColumn('open_on_dlv_day_rate_m'+i+'1', 
                         get_delivery_rate_udf("last_open_day_cnt_m"+i+'1', "delivery_all_cnt_m"+i+'1'))
#     sdf = sdf.withColumn('open_on_dlv_day_dist_rate_m'+i+'1', 
#                          get_delivery_rate_udf("last_open_day_cnt_dist_m"+i+'1', "delivery_all_cnt_m"+i+'1'))    
#     sdf = sdf.withColumn('click_day_rate_m'+i, get_delivery_rate_udf("last_click_time_sum_m"+i, "deliverycnt"))
    sdf = sdf.withColumn('clk_on_dlv_day_rate_m'+i+'1', 
                         get_delivery_rate_udf("last_click_day_cnt_m"+i+'1', "delivery_all_cnt_m"+i+'1'))
#     sdf = sdf.withColumn('clk_on_dlv_day_dist_rate_m'+i+'1', 
#                          get_delivery_rate_udf("last_click_day_cnt_dist_m"+i+'1', "delivery_all_cnt_m"+i+'1')) 
    sdf = sdf.withColumn('clk_on_open_day_rate_m'+i+'1', 
                         get_delivery_rate_udf("last_click_day_cnt_m"+i+'1', "last_open_day_cnt_m"+i+'1'))
#     sdf = sdf.withColumn('clk_on_open_day_dist_rate_m'+i+'1', 
#                          get_delivery_rate_udf("last_click_day_cnt_dist_m"+i+'1', "last_open_day_cnt_dist_m"+i+'1'))    

import locale
from functools import cmp_to_key

customer_attr = ['customer_text_0{}'.format(str(i)) if i<10 
                 else 'customer_text_{}'.format(str(i)) for i in range(1,21)]

first_order_cols = \
[
    'inn', 
    'kpp',
    'campaign_nm', 
    'email',
    'organization_id',
    'custid_value',
    'response',
    'response_dttm',
    'asday',
    'asdayname',
    'ashour',
    'astime',     
    'email_subject',
    'message_title',]\
+\
customer_attr

reordered_cols = \
first_order_cols + sorted(set(sdf.columns) -
                          set(first_order_cols) -
                          set(['load_dt','last_day']), key=cmp_to_key(locale.strcoll)) + ['last_day']

sdf = sdf.select(*reordered_cols)


# Cut all daytime events that aren't related to the Test datased (cut records in window of 3month back from min responce date)
sdf = sdf.filter((f.col('response_dttm') >= resp_dttm_min_str)) 


# Remove events with only delivery status among users with para fresh_user = 0
sdf = sdf.filter("(response != 'SendSay. Сообщение доставлено' and fresh_user = 0) or (sum_open_click_camp = 0 and fresh_user = 0) or (fresh_user != 0)")


# Generate features required for best day/time calculation
best_time_sdf = sdf \
    .withColumn('day_open_cnt', 
                    f.sum(f.when(f.col('response') == sendsay_openpage, 1).otherwise(0))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email', 'asdayname')).cast('integer'))\
    .withColumn('day_hour_open_cnt', 
                    f.sum(f.when(f.col('response') == sendsay_openpage, 1).otherwise(0))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email', 'asdayname', 'ashour')).cast('integer'))\
    .select(*['inn', 'kpp', 'campaign_nm', 'email', 'response', 'asdayname', 'ashour', 'day_open_cnt', 'day_hour_open_cnt'])
    
best_time_sdf = best_time_sdf.filter(''' response != "SendSay. Сообщение доставлено" ''')\
         .withColumn('top_day', f.dense_rank().over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email', 'asdayname')\
                                              .orderBy(f.col('day_open_cnt').desc())))\
                                              .filter('''top_day = 1''')    
    
best_time_sdf = best_time_sdf.\
        withColumn('top_hour', f.dense_rank().over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email', 'asdayname')\
                                             .orderBy(f.col('day_hour_open_cnt').desc()))).filter('''top_hour = 1''')
    
best_time_sdf = best_time_sdf.drop(*['top_day', 'top_hour', 'day_hour_open_cnt', 'response'])


# Groupby up to email (keep on one first record per one email)
best_time_sdf = best_time_sdf.groupBy('inn', 'kpp', 'campaign_nm', 'email').agg(f.first('asdayname').alias('asdayname'), 
                                                                                f.first('ashour').alias('ashour'),
                                                                                f.first('day_open_cnt').alias('day_open_cnt'))

best_time_sdf.cache()


# Select users with flag equals 1
strdate = datetime.strftime(datetime.now(), format='%Y.%d.%m %H:%M:%S')
currdate = datetime.strptime(strdate,'%Y.%d.%m %H:%M:%S')

sdf_fresh = sdf.filter((f.col('response_dttm') >= resp_dttm_min_str) & (f.col('fresh_user')==1)).select('inn', 
                                                                                                        'kpp', 
                                                                                                        'campaign_nm', 
                                                                                                        'email', 
                                                                                                        'fresh_user')
sdf_fresh = sdf_fresh.withColumn('email_proba',f.lit(0.0).cast(FloatType()))
sdf_fresh = sdf_fresh.withColumn('email_class',f.lit(-1).cast(IntegerType()))   

sdf_fresh = sdf_fresh.withColumn('load_dt', f.lit(currdate).cast(TimestampType()))

sdf_fresh.cache()


sp.db = 'sbx_team_digitcamp'
sp.output = 'tmp_preproc_resp_inn_kpp_camp'
sp.save(sdf)


# Load preproc temp table
sdf = hive.sql('''select * from sbx_team_digitcamp.tmp_preproc_resp_inn_kpp_camp''')
sdf.registerTempTable('temp_resp_inn_kpp_camp')


# Join with ML360
# ml360 = 'dl_t_team_ml360'
ml360 = 'cib_custom_cib_ml360'

sdf = hive.sql('''
/*with mon_products as
(        select * from {tbl}.mon_products
         union select * from dl_t_team_ml360.mon_products_20200414033929
         union select * from dl_t_team_ml360.mon_products_20200629064208
         union select * from dl_t_team_ml360.mon_products_20200703111831
         union select * from dl_t_team_ml360.mon_products_20200707095006
)*/

select 
t.*,
concat_ws(' ',email_subject, message_title, 
            customer_text_03, customer_text_04, customer_text_05, 
            customer_text_06, customer_text_07, customer_text_08, customer_text_09, customer_text_10,
                              customer_text_12, customer_text_13, customer_text_14, customer_text_15, 
                              customer_text_17, customer_text_18, customer_text_19, customer_text_20) as text
from(
         select 
             rik.inn
            ,rik.kpp
            ,rik.campaign_nm
            ,rik.email
            --,rik.target            
            ,rik.organization_id
            ,rik.custid_value
            ,rik.response
            ,rik.response_dttm
            ,rik.asday
            ,rik.asdayname
            ,rik.ashour
            ,rik.astime           
            ,rik.email_subject
            ,rik.message_title
            ,rik.customer_text_01
            ,rik.customer_text_02
            ,rik.customer_text_03
            ,rik.customer_text_04
            ,rik.customer_text_05
            ,rik.customer_text_06
            ,rik.customer_text_07
            ,rik.customer_text_08
            ,rik.customer_text_09
            ,rik.customer_text_10
            ,rik.customer_text_11
            ,rik.customer_text_12
            ,rik.customer_text_13
            ,rik.customer_text_14
            ,rik.customer_text_15
            ,rik.customer_text_16
            ,rik.customer_text_17
            ,rik.customer_text_18
            ,rik.customer_text_19
            ,rik.customer_text_20
            ,rik.clk_on_dlv_all_rate_m11
            ,rik.clk_on_dlv_all_rate_m21
            ,rik.clk_on_dlv_all_rate_m31
            ,rik.clk_on_dlv_day_rate_m11
            ,rik.clk_on_dlv_day_rate_m21
            ,rik.clk_on_dlv_day_rate_m31
            ,rik.clk_on_open_all_rate_m11
            ,rik.clk_on_open_all_rate_m21
            ,rik.clk_on_open_all_rate_m31
            ,rik.clk_on_open_day_rate_m11
            ,rik.clk_on_open_day_rate_m21
            ,rik.clk_on_open_day_rate_m31
            ,rik.delivery_all_cnt_m11
            ,rik.delivery_all_cnt_m21
            ,rik.delivery_all_cnt_m31
            ,rik.diff_fcl_fo_m11
            ,rik.diff_fcl_fo_m21
            ,rik.diff_fcl_fo_m31
            ,rik.diff_fcl_fs_m11
            ,rik.diff_fcl_fs_m21
            ,rik.diff_fcl_fs_m31
            ,rik.diff_fcl_lo_m11
            ,rik.diff_fcl_lo_m21
            ,rik.diff_fcl_lo_m31
            ,rik.diff_fcl_ls_m11
            ,rik.diff_fcl_ls_m21
            ,rik.diff_fcl_ls_m31
            ,rik.diff_fo_fs_m11
            ,rik.diff_fo_fs_m21
            ,rik.diff_fo_fs_m31
            ,rik.diff_fo_ls_m11
            ,rik.diff_fo_ls_m21 --Important features!
            ,rik.diff_fo_ls_m31
            ,rik.diff_lcl_fcl_m11
            ,rik.diff_lcl_fcl_m21
            ,rik.diff_lcl_fcl_m31
            ,rik.diff_lcl_fo_m11
            ,rik.diff_lcl_fo_m21
            ,rik.diff_lcl_fo_m31
            ,rik.diff_lcl_fs_m11
            ,rik.diff_lcl_fs_m21
            ,rik.diff_lcl_fs_m31
            ,rik.diff_lcl_lo_m11
            ,rik.diff_lcl_lo_m21
            ,rik.diff_lcl_lo_m31
            ,rik.diff_lcl_ls_m11
            ,rik.diff_lcl_ls_m21
            ,rik.diff_lcl_ls_m31
            ,rik.diff_lo_fo_m11 --Important features!
            ,rik.diff_lo_fo_m21 --Important features!
            ,rik.diff_lo_fo_m31
            ,rik.diff_lo_fs_m11 --Important features!
            ,rik.diff_lo_fs_m21
            ,rik.diff_lo_fs_m31
            ,rik.diff_lo_ls_m11 --Important features!
            ,rik.diff_lo_ls_m21 --Important features!
            ,rik.diff_lo_ls_m31
            ,rik.first_click_time
            ,rik.first_click_time_m11
            ,rik.first_click_time_m21
            ,rik.first_click_time_m31
            ,rik.first_open_time
            ,rik.first_open_time_m11
            ,rik.first_open_time_m21
            ,rik.first_open_time_m31
            ,rik.first_send_time
            ,rik.first_send_time_m11
            ,rik.first_send_time_m21
            ,rik.first_send_time_m31
            ,rik.fresh_user
            ,rik.last_click_all_cnt_m11 --Important features!
            ,rik.last_click_all_cnt_m21
            ,rik.last_click_all_cnt_m31
            ,rik.last_click_day_cnt_m11
            ,rik.last_click_day_cnt_m21
            ,rik.last_click_day_cnt_m31
            ,rik.last_click_time
            ,rik.last_click_time_m11
            ,rik.last_click_time_m21
            ,rik.last_click_time_m31
            ,rik.last_open_all_cnt_m11 --Important features!
            ,rik.last_open_all_cnt_m21 --Important features!
            ,rik.last_open_all_cnt_m31 --
            ,rik.last_open_day_cnt_m11 --Important features!
            ,rik.last_open_day_cnt_m21 --
            ,rik.last_open_day_cnt_m31
            ,rik.last_open_time
            ,rik.last_open_time_m11
            ,rik.last_open_time_m21
            ,rik.last_open_time_m31
            ,rik.last_send_time
            ,rik.last_send_time_m11
            ,rik.last_send_time_m21
            ,rik.last_send_time_m31
            ,rik.open_on_dlv_all_rate_m11 --Important features!
            ,rik.open_on_dlv_all_rate_m21 --Important features!
            ,rik.open_on_dlv_all_rate_m31
            ,rik.open_on_dlv_day_rate_m11 --Important features!
            ,rik.open_on_dlv_day_rate_m21
            ,rik.open_on_dlv_day_rate_m31
            ,rik.sum_open_click_camp
            --,rik.load_dt
            ,rik.last_day

            --,mcs.cred_sdo
            --,mcs.weight_prc
            --,mcs.rate_diff

            ,mdh.all_prod_deals_created
            ,mdh.all_prod_deals_completed
            ,mdh.credit_prod_deals_created
            ,mdh.merch_prod_deals_created
            ,mdh.corp_cards_prod_deals_created --Important features!
            ,mdh.salary_prod_deals_created --Important features!
            ,mdh.depozit_prod_deals_created
            ,mdh.insure_prod_deals_created
            ,mdh.credit_prod_deals_completed
            ,mdh.merch_prod_deals_completed
            ,mdh.corp_cards_prod_deals_completed --Important features!
            ,mdh.salary_prod_deals_completed
            ,mdh.depozit_prod_deals_completed
            ,mdh.insure_prod_deals_completed
            --,mdh.segment

            --,mibs.complicity_type
            --,mibs.otrasl
            --,cast(mibs.ul_kopf_cd as int) ul_kopf_cd
            --,mibs.big_segment

            --,mip.oborot_kredit
            --,mip.invest_kredit
            --,mip.overdraft_kredit
            --,mip.proekt_finans
            --,mip.products_lizing
            --,mip.obsluzh_rts_rur
            --,mip.obsluzh_rts_val
            --,mip.spec_acc
            --,mip.deposit
            --,mip.veksel
            --,mip.deposit_sertificat
            --,mip.inkass
            --,mip.samoinkass
            --,mip.zarplat_projects
            --,mip.acquiring_torg
            --,mip.acquiring_mobile
            --,mip.acreditiv
            --,mip.valuta_control
            --,mip.garantee_kontract
            --,mip.garantee_gos
            --,mip.garantee_other
            --,mip.arenda_seyf
            --,mip.dbo
            --,mip.corporate_cards
            --,mip.cash_management
            --,mip.bank_straxovanie_imush
            --,mip.bank_straxovanie_lich
            --,mip.bank_straxovanie_otvet
            --,mip.einvoicing
            --,mip.factoring
            --,mip.konversion
            --,mip.ns_servis
            --,mip.ns_terminal

            ,mnbc.segment crm_segment
            ,mnbc.prior_true --Important features!
            ,mnbc.OPF_IP
            ,mnbc.OPF_OOO
            ,mnbc.OPF_Other
            ,mnbc.tb_Centralno_Chernozemnyj
            ,mnbc.tb_Moskovskij
            ,mnbc.tb_Uralskij
            ,mnbc.tb_Povolzhskij
            ,mnbc.tb_Severo_Zapadnyj
            ,mnbc.tb_Dalnevostochnyj
            ,mnbc.tb_Volgo_Vyatskij
            
            --,mp.corp_cards
            --,mp.rko
            --,mp.salary
            --,mp.merch
            --,mp.credit
            --,mp.deposits            

            --,yfp.egr_org_id
            --,yfp.ip_flg
            --,yfp.ul_org_id
            --,yfp.egrul_org_id
            --,yfp.ogrn
            --,yfp.short_nm
            --,yfp.okato_cd
            --,yfp.kopf_cd
            --,yfp.kopf_nm
            --,cast(yfp.active_flg as int) active_flg
            --,yfp.founders_all_cnt
            --,yfp.founders_ul_cnt
            --,yfp.head_inn
            --,yfp.founders_fl_cnt
            --,yfp.founders_foreign_ul_cnt
            --,yfp.branch_cnt
            --,yfp.adrs_index
            --,yfp.adrs_rf_subject
            --,yfp.egrip_org_id
            --,yfp.ip_org_id
            --,yfp.ar_revenue
            --,yfp.ar_total_expenses
            --,yfp.ar_profit_before_tax
            --,yfp.ar_taxes
            --,yfp.ar_net_profit
            --,yfp.fot_balance
            --,yfp.license_cnt
            --,yfp.okfs_cd
            --,yfp.integrum_staff_range
            --,yfp.ab_immobilized_assets
            --,yfp.ab_losses
            --,yfp.ab_own_capital
            --,yfp.ab_borrowed_capital
            --,yfp.okved_cnt
            --,yfp.industry
            --,yfp.total_sum_kt
            --,yfp.total_count_kt
            --,yfp.total_sum_dt
            --,yfp.total_count_dt
            --,yfp.total_sum_kt_3m
            --,yfp.total_count_kt_3m
            --,yfp.total_sum_dt_3m
            --,yfp.total_count_dt_3m
            --,yfp.total_sum_kt_year18
            --,yfp.total_count_kt_year18
            --,yfp.total_sum_dt_year18
            --,yfp.total_count_dt_year18
            --,yfp.tax_income
            --,yfp.penalty_income
            --,yfp.tax_ip
            --,yfp.penalty_ip
            --,yfp.tax_foreign
            --,yfp.penalty_foreign
            --,yfp.tax_income_3m
            --,yfp.penalty_income_3m
            --,yfp.tax_ip_3m
            --,yfp.penalty_ip_3m
            --,yfp.tax_foreign_3m
            --,yfp.penalty_foreign_3m
            --,yfp.tax_income_year18
            --,yfp.penalty_income_year18
            --,yfp.tax_ip_year18
            --,yfp.penalty_ip_year18
            --,yfp.tax_foreign_year18
            --,yfp.penalty_foreign_year18
            --,yfp.kpp_regions_cnt
            --,yfp.gis_building_area
            --,yfp.gis_merchant_cnt
            --,yfp.gis_staff_cnt
            --,yfp.gis_building_firms
            --,yfp.gis_network_flg
            --,yfp.gis_rosn_merchant_flg
            --,yfp.gis_opt_merchant_flg
            --,yfp.gis_production_merchant_flg
            --,yfp.gis_internet_merchant_flg
            --,yfp.gis_bank_payment_flg
            --,yfp.gis_cash_payment_flg
            --,yfp.gis_card_payment_flg
            --,yfp.gis_internet_payment_flg
            --,yfp.cred_application_cnt
            --,yfp.segment fot_segment
            --,yfp.okato_reg
            --,yfp.integrum_lower_bound
            --,yfp.wlt_prediction
            --,yfp.fns_staff_cnt
            --,yfp.mean_zp
            --,yfp.zp_amount
            --,yfp.mean_zp_empl_cnt
            --,yfp.max_zp_empl_cnt
            --,yfp.min_zp_empl_cnt
            --,yfp.stoplist
              
         from temp_resp_inn_kpp_camp    rik
         --left join {tbl}.mon_cred_sdo                mcs  on rik.inn = mcs.inn  and rik.last_day = mcs.mon
         left join {tbl}.mon_deals_history           mdh  on rik.inn = mdh.inn  and rik.last_day = mdh.mon
         --left join dl_t_team_ml360.mon_iskra_big_segment       mibs on rik.inn = mibs.inn and rik.last_day = mibs.mon
         --left join dl_t_team_ml360.mon_iskra_prod              mip  on rik.inn = mip.inn  and rik.last_day = mip.mon
         left join {tbl}.mon_nba_base_crm            mnbc on rik.inn = mnbc.inn and rik.last_day = mnbc.mon
         --left join mon_products                      mp   on rik.inn = mp.inn   and rik.last_day = mp.mon  
         --left join dl_t_team_ml360.yr_fot_profile              yfp  on rik.inn = yfp.inn  and rik.last_day = yfp.yr  
         --                                                                       and rik.kpp = yfp.kpp      
         
         where fresh_user in (0,2)
         --and load_dt <= timestamp('%s')
) t
--where target in (0,1)
'''.format(tbl=ml360)) #%(prev_load_dt_str))


# Join with CALL_TASK_TO_SENDSAY
call_sdf = hive.sql("select cast( cast(inn as long) as string) _inn, CREATE_DATE from sbx_team_digitcamp.CALL_TASK_TO_SENDSAY")

sdf = sdf.join(call_sdf, on=( (sdf.inn==call_sdf._inn) & 
                              (call_sdf.CREATE_DATE >= f.date_sub(sdf.first_send_time, 40))))

sdf = sdf.withColumn('call_first_check', f.when((f.col('CREATE_DATE') < f.col('last_send_time')) & \
                                                (f.col('CREATE_DATE') >= f.date_sub(f.col('last_send_time'),30)), 1).otherwise(0))

sdf.cache()


# Preproc
sdf = sdf.withColumn('idx', f.monotonically_increasing_id())

import locale
import re
from functools import cmp_to_key

# indx_cols = list(zip(range(len(final_cols)),final_cols))

customer_attr = ['customer_text_0{}'.format(str(i)) if i<10 
                 else 'customer_text_{}'.format(str(i)) for i in range(1,21)]

first_order_cols = \
[
    'inn',
    'kpp', 
    'organization_id',
    'campaign_nm',
    'email',
    #'target'
]

rem_cols = \
[
#'sum_open_click_camp', 
'custid_value', 
'CREATE_DATE',
'_inn',
'response', 'asday','astime', 'ashour', 'asdayname', 'yr',
'email_subject', 'message_title',
'wlt_prediction',
'adrs_index', 'adrs_rf_subject', 'head_inn', 'kopf_nm', 'short_nm',
'last_day',
'mon', 'okato_cd', 'kopf_cd','okfs_cd','okved_cnt','egrip_org_id','ip_org_id','integrum_staff_range',
'gis_building_area','gis_staff_cnt','gis_building_firms','okato_reg','fns_staff_cnt',
'load_dt', 'obsluzh_rts_rur','salary','corp_cards', 'ogrn'
]


reordered_cols = first_order_cols+ \
sorted(set(sdf.columns) - set(customer_attr)
                        - set(first_order_cols)
                        - set(rem_cols), key=cmp_to_key(locale.strcoll))

pattern = re.compile(r'.*_click_time.*|.*_open_time.*|.*_send_time.*|delivery_all_cnt.*')
# 'delivery_all_cnt_m11','delivery_all_cnt_m21'

final_cols = [col for col in reordered_cols if len(pattern.findall(col)) == 0]

sdf = sdf.select(*final_cols)


# Fill NAN
replna = {}
for col in sdf.dtypes:
    if col[1] == 'string':
        replna.update({col[0]:''})
    elif col[1] == 'boolean':
        replna.update({col[0]:False})
    elif col[1] != 'timestamp':
        replna.update({col[0]:0})

sdf = sdf.fillna(replna)


# Load Prefitted PipelineModel
pipelineModelPreFit = PipelineModel.load("hdfs:///user/{}/sendsay_pipeline_StringIndexer_upd".format(curruser))
sdf = pipelineModelPreFit.transform(sdf)

# drop string columns
catcol = ['big_segment', 'crm_segment', 'fot_segment', 'complicity_type', 'otrasl', 'prior_true'] 
catcol.extend(['response_dttm'])
sdf = sdf.drop(*catcol)


# Remove constant columns
with open('./../RESP_MODELS/csv/zero_std_cols', 'r') as fin:
    res = fin.readlines()
zero_std_cols = [re.sub('\\n','',line) for line in res]
sdf = sdf.drop(*zero_std_cols)


# Preprocessing with FastText
import string

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer, word_tokenize

import pymorphy2
# morph = pymorphy2.MorphAnalyzer()
# broadcast 
MORPH = pymorphy2.MorphAnalyzer()
morph_broadcast = sp.sc.broadcast(MORPH)

stop_words = set(stopwords.words('russian'))
exclude = set(string.punctuation) 

def text_prepare(text, lemmatize = False): 
    morph = morph_broadcast.value
    def drop_nps(tokenslst, lemmatize=lemmatize):
        lres=[]
        for token in tokenslst:
            parse_res=morph.parse(token)[0]
            if not 'Name' in parse_res.tag and \
               not 'Patr' in parse_res.tag and \
               not 'Surn' in parse_res.tag:
                        if not lemmatize:
                            lres.append(parse_res.word)
                        else:
                            lres.append(parse_res.normal_form)      
        return ' '.join(lres)

    text = ''.join(x if x not in exclude else ' ' for x in text)
    text = ' '.join(word for word in text.split() if word not in stop_words)
    text = re.sub(r'[a-zA-Z\d{1,8}]+','',text)
    text = ' '.join(word for word in text.split() if len(word)>2) 
    tokenslst = list(map(lambda x: x.replace('"', '').lower(), RegexpTokenizer('([а-яА-Яa-zA-Z\d{9,12}]+|\"[\w\s]+\")').tokenize(text)))

    if not lemmatize:
        res = drop_nps(tokenslst)
    else:    
        res = drop_nps(tokenslst, True)    
    return res

text_prepare_udf = f.udf(text_prepare, StringType())
sdf = sdf.withColumn('text_norm', text_prepare_udf('text'))  


# TFIDF+FastText: Run broadcast
from pyspark import SparkFiles

hdfsfile='hdfs:///user/{}/email_sendsay_TfIdf_vect.pkl'.format(curruser)
sp.sc.addFile(hdfsfile)
embed = joblib.load(SparkFiles.get('email_sendsay_TfIdf_vect.pkl'))

rev_dct = sp.sc.broadcast(embed.gensim_rev_dct)
tfIdfModel = sp.sc.broadcast(embed.gensim_tfIdfModel)
bow = sp.sc.broadcast(embed.bow)
model_word_tfidf = sp.sc.broadcast(embed.model_word_tfidf)
model_char_tfidf = sp.sc.broadcast(embed.model_char_tfidf)

col_bc = sp.sc.broadcast(sdf.columns)

colexp = col_bc.value
colexp.remove('text')
colexp.remove('text_norm')

sp.sc.addFile('./../src/corpora_transform_main.py')

#!hdfs dfs -put model/ft_vector /user/ufimtsev1-ys_ca-sbrf-ru/
hdfsfile = 'hdfs:///user/{}/FT/ft_vector'.format(curruser)
try:
    sp.sc.addFile(hdfsfile)
except:
    print('File exists in directory')

# !hdfs dfs -put model/ft_vector.vectors_ngrams.npy /user/ufimtsev1-ys_ca-sbrf-ru/
# hdfsfile = 'hdfs:///user/ufimtsev1-ys_ca-sbrf-ru/ft_vector.vectors_ngrams.npy'
# sp.sc.addFile(hdfsfile)

#!hdfs dfs -put model/ft.word2vec /user/ufimtsev1-ys_ca-sbrf-ru/
hdfsfile = 'hdfs:///user/{}/FT/ft.word2vec'.format(curruser)
sp.sc.addFile(hdfsfile)

def block_iterator(iterator,size):
    """
    dest:
        сервисная функция для итерации по блокам данных внутри RDD. 
        Чем больше size при увеличении количества потоков, тем быстрее обработка
    args:
        iterator-объект 
        size - размер элементов для единичной итерации
    return:
        вычисляемый объект bucket
 
    """
    bucket = list()
    for e in iterator:
        bucket.append(e)
        if len(bucket) >= size:
            yield bucket
            bucket = list()
    if bucket:
        yield bucket
        
        
# TfIdfFastTextEmbedVectorizer: Generate Features Based on FastText Vector Only        
def block_classify(iterator):
    curruser = os.environ.get('USER')
    if isUseOptWorkspace:
#         sys.path.insert(0,'/home/{}/.local/lib/python3.5/site-packages/'.format(curruser))
        sys.path.insert(0,'/opt/workspace/{}/libs/python3.5/site-packages'.format(curruser))
    else:
        sys.path.insert(0,'/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
    from corpora_transform_main import TfIdfFastTextEmbedVectorizer
    from gensim.models.fasttext import FastText as FT_gensim
    from gensim.models import KeyedVectors
    import pandas as pd
    import json

    for features in block_iterator(iterator, 1000000):
        
        features_df = pd.DataFrame(list(features), columns = col_bc.value) # получаем исходные данные в виде df        
#         X_predict = features_df[[col for col in features_df.columns if col not in ['inn', 'kpp', 'campaign_nm', 'text']]]    
        X_predict = features_df['text_norm']
    
#         model_ft = FT_gensim.load(SparkFiles.get('ft_vector'))
        model_ft = KeyedVectors.load_word2vec_format(SparkFiles.get('ft.word2vec'))
        _useTfIdf2Pred = False
        embed = TfIdfFastTextEmbedVectorizer(model = model_ft, 
                                             useTfIdf2Pred=_useTfIdf2Pred,
                                             word_ngram_range=(1,2), 
                                             word_max_vec_features=300)
        
        embed.gensim_rev_dct = rev_dct.value 
        embed.gensim_tfIdfModel = tfIdfModel.value
        embed.bow = bow.value
        
        if _useTfIdf2Pred:
            embed.model_word_tfidf = model_word_tfidf.value
            embed.model_char_tfidf = model_char_tfidf.value

        res = embed.transform(X_predict)
        ft_df = pd.DataFrame(res, columns=embed.feature_names)
        
        res_df = features_df[colexp]
        res_df = pd.concat([res_df,ft_df], axis=1)
 
        for e in json.loads(res_df.to_json(orient='records')): # генерируем выходной блок
            yield e
                  
sdf_proc = sdf.rdd.mapPartitions(block_classify)

rdd  = sdf.limit(1).rdd.mapPartitions(block_classify)
flat = rdd.flatMap(lambda x: [key for key in x.keys()]).collect()

ft_cols = [key for key in flat if '_ft_' in key.lower()]

typesmap_rdd={}

for column_name, column in sdf.dtypes:
    if column == 'string':
        typesmap_rdd[column_name] = StringType()
    elif 'decimal' in column:
#         digits = int('{}'.format(column.split('(')[1].split(',')[0]))
#         prec   = int('{}'.format(column.split('(')[1].split(',')[1][:-1]))
        typesmap_rdd[column_name] = FloatType() #DecimalType(digits,prec)
    elif column == 'double':
        typesmap_rdd[column_name] = DoubleType()        
    elif column == 'float':
        typesmap_rdd[column_name] = FloatType()
    elif column == 'int':
        typesmap_rdd[column_name] = IntegerType()       
    elif column == 'bigint':
        typesmap_rdd[column_name] = LongType()   

for column_name in ft_cols:        
       typesmap_rdd[column_name] = FloatType() 
        
cols = colexp+ft_cols
currschema = StructType([StructField(col, typesmap_rdd[col]) for col in cols])
sdf_proc = sp.sql.createDataFrame(sdf_proc, schema=currschema)      


# Load PreTrained Xgboost model
import xgboost
from xgboost import XGBClassifier
from sklearn.preprocessing import binarize

# file = 'xgb_best_param_wFT_10M_200613.pkl'
filename =  open(os.path.join('./../RESP_MODELS/pkl/', 'best_model.inf'), "r", encoding='cp1251').read()
fileinfo = re.split(';', filename.rstrip('\n'))
model = joblib.load('./../RESP_MODELS/pkl/{}'.format(fileinfo[0]))


# Make a Prediction: Run broadcast
bstBroadcast = sp.sc.broadcast(model)
col_bc = sp.sc.broadcast(sdf_proc.columns)
threshold_bc = sp.sc.broadcast(fileinfo)

def block_classify(iterator):
    import os
    curruser = 'ektov1-av_ca-sbrf-ru'
    if isUseOptWorkspace:
#         sys.path.insert(0,'/home/{}/.local/lib/python3.5/site-packages/'.format(curruser))
        sys.path.insert(0,'/opt/workspace/{}/libs/python3.5/site-packages'.format(curruser))
    else:
        sys.path.insert(0,'/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
    import xgboost
    import pandas as pd
    import json
    import re
    import ast
    
    from sklearn.preprocessing import binarize

    for features in block_iterator(iterator, 500000):
        
        features_df = pd.DataFrame(list(features), columns = col_bc.value) # получаем исходные данные в виде df 
        
#         pattern = re.compile(r'diff_fo_fs.*|diff_lo_fs.*|diff_fcl_fo.*|diff_fcl_fs.*|diff_lcl_fo.*|diff_lcl_fs.*|diff_fcl_lo.*|.*_day_.*|corp_cards_prod_deals.*')

        delcols = \
         ['inn', 
          'kpp', 
          'campaign_nm',
          'email',
          'organization_id', 
          'idx', 
          'fresh_user',  
          'response_dttm'
          'target',
          'sum_open_click_camp', 
          ]         
        
#         filter_cols = [col for col in features_df.columns if (len(pattern.findall(col)) == 0) and (col not in delcols)]
        filter_cols = [col for col in features_df.columns if (col not in delcols)]
#         sortcols = sorted(filter_cols)
#         X_predict = features_df[sortcols]
        X_predict = features_df[filter_cols]
        
        threshold = float(threshold_bc.value[2]) # get threshold for model
        
        try:
            predicted_proba = bstBroadcast.value.predict_proba(X_predict.astype(float))[:,1]
#             predicted       = bstBroadcast.value.predict(X_predict.astype(float))
            predicted = binarize(predicted_proba.reshape(-1, 1), threshold=threshold).reshape(-1)
        except ValueError as err:
            colsstr = str(err).split('feature_names mismatch: ')[1].split(' [')[0]
            getcolslist = list(ast.literal_eval(colsstr[1:-1]))
            X_predict = features_df[getcolslist]       
            predicted_proba = bstBroadcast.value.predict_proba(X_predict.astype(float))[:,1]
#             predicted       = bstBroadcast.value.predict(X_predict.astype(float))
            predicted = binarize(predicted_proba.reshape(-1, 1), threshold=threshold).reshape(-1)

        res_df                = features_df[['campaign_nm']]
        res_df["email"]       = features_df[['email']]
        res_df["inn"]         = features_df[['inn']]
        res_df["kpp"]         = features_df[['kpp']]
        res_df["email_proba"] = predicted_proba
        res_df["email_class"] = predicted
        res_df["fresh_user"]  = features_df[['fresh_user']]
        res_df = res_df[['inn','kpp', 'email', 'campaign_nm','email_proba','email_class', 'fresh_user']]

        for e in json.loads(res_df.to_json(orient='records')): # генерируем выходной блок
            yield e
print('XGB result')            
scores = sdf_proc.rdd.mapPartitions(block_classify).toDF()


# Add some specific columns
scores = scores.select([f.col('inn'),
                        f.col('kpp'),
                        f.col('campaign_nm'),
                        f.col('email'),
                        f.col('email_proba').cast(FloatType()),
                        f.col('email_class').cast(IntegerType()),
                        f.col('fresh_user')])
scores = scores.withColumn('load_dt', f.lit(currdate).cast(TimestampType()))


# Join scored users with inactive subset
sdf_fresh = sdf_fresh[scores.columns]
fin_sdf = scores.distinct().union(sdf_fresh)

# Agregating by INN KPP CAMP EMAIL and keep only best probabilities

fin_sdf = fin_sdf\
         .withColumn('top_proba', f.dense_rank().over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')\
                                                .orderBy(f.col('email_proba').desc())))\
                                                .filter('''top_proba = 1''') 
        
# fin_sdf = fin_sdf.rdd.sortBy(lambda x: (x[0], x[1], x[2], x[3], x[4]), ascending=False).toDF()
fin_sdf = fin_sdf.groupBy(['inn', 'kpp', 'campaign_nm', 'email']).agg(f.first(f.col('email_proba')).alias('email_proba'), 
                                                                      f.first(f.col('email_class')).alias('email_class'),
                                                                      f.first(f.col('fresh_user')).alias('fresh_user'),
                                                                      f.first(f.col('load_dt')).alias('load_dt'))

fin_sdf = fin_sdf.select('inn', 
                         'kpp', 
                         'campaign_nm', 
                         'email', 
                         'email_proba', 
                         'email_class', 
                         'fresh_user', 
                         'load_dt')

fin_sdf = fin_sdf.select(*[f.col(col).alias(col+'_') for col in fin_sdf.columns])


# Join Scores with Best Day/Hour table
conditions= ((fin_sdf.inn_==best_time_sdf.inn)&
             (fin_sdf.kpp_==best_time_sdf.kpp)&
             (fin_sdf.campaign_nm_==best_time_sdf.campaign_nm)&
             (fin_sdf.email_==best_time_sdf.email))

res = fin_sdf.join(best_time_sdf, on=conditions, how='left_outer')

res = res.drop(*['inn', 'kpp', 'campaign_nm', 'email'])

res = res.drop_duplicates()


# UPDATE: FOR EACH FOLLOWING CASES: Perform Full Oter Join with RESP_INN_KPP_CAMP_EMAIL_PROBA
res.registerTempTable('tmp_scores')
hive.cacheTable('tmp_scores')

import subprocess

sdf_resp = hive.sql("select * from sbx_team_digitcamp.RESP_INN_KPP_CAMP_EMAIL_PROBA")

alter_spdf = \
hive.sql('''
select 
 inn
,kpp
,campaign_nm
,email
,email_proba
,email_class
,fresh_user
,asdayname
,ashour
,day_open_cnt
,load_dt
from(
        select 
            t.*,
            row_number() over (partition by inn, 
                                            kpp,
                                            campaign_nm,
                                            email, 
                                            email_proba 
                                                         order by load_dt) rownum
        from(
                select
                    coalesce(p.inn, b.inn_) inn,
                    coalesce(p.kpp, b.kpp_) kpp,
                    coalesce(p.campaign_nm, b.campaign_nm_) campaign_nm,
                    coalesce(p.email, b.email_) email,
                    coalesce(b.email_proba_, p.email_proba) email_proba,
                    coalesce(b.email_class_, p.email_class) email_class,
                    coalesce(b.fresh_user_, p.fresh_user) fresh_user,
                    coalesce(b.asdayname, p.asdayname) asdayname,
                    coalesce(b.ashour, p.ashour) ashour,
                    coalesce(b.day_open_cnt, p.day_open_cnt) day_open_cnt,
                    coalesce(b.load_dt_, p.load_dt) load_dt
                from sbx_team_digitcamp.RESP_INN_KPP_CAMP_EMAIL_PROBA p
                full outer join tmp_scores b on
                p.inn = b.inn_ and p.kpp = b.kpp_ and
                p.email = b.email_
                
        ) t
) where rownum = 1 ''')



table_name = 'RESP_INN_KPP_CAMP_EMAIL_PROBA'
tmp_tbl = 'tmp_{}'.format(table_name)

alter_spdf.registerTempTable(tmp_tbl)

hive.sql('''drop table if exists sbx_team_digitcamp.{}__ purge'''.format(table_name))
hive.sql('''create table sbx_team_digitcamp.{}__ select * from {}'''.format(table_name,tmp_tbl))
hive.sql('''drop table if exists sbx_team_digitcamp.{} purge'''.format(table_name))
subprocess.call(['hdfs', 'dfs', '-rm', '-R', '-skipTrash', "hdfs://clsklsbx/user/team/team_digitcamp/hive/{}".format(table_name.lower())])
hive.sql('''alter table sbx_team_digitcamp.{}__ rename to sbx_team_digitcamp.{}'''.format(table_name, table_name))
hive.sql("drop table if exists {} purge".format(tmp_tbl))




# resp_inn_kpp_best_proba
strdate = datetime.strftime(datetime.now(), format='%Y.%d.%m %H:%M:%S')
currdate = datetime.strptime(strdate,'%Y.%d.%m %H:%M:%S')

table_name = 'RESP_INN_KPP_CAMP_EMAIL_PROBA'
spdf = hive.sql("select * from sbx_team_digitcamp.{}".format(table_name))


spdf = spdf\
    .withColumn('email_cnt', 
                    f.count(f.col('email'))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm', 'email')).cast('integer'))

spdf = spdf\
    .withColumn('email_cnt_dist', 
                    f.approx_count_distinct(f.col('email'))\
                    .over(Window.partitionBy('inn', 'kpp', 'campaign_nm')).cast('integer'))
    
# Agregating by INN KPP and keep only best probabilities
spdf = spdf\
         .withColumn('top_proba', f.dense_rank().over(Window.partitionBy('inn', 'kpp')\
                                                .orderBy(f.col('email_proba').desc())))\
                                                .filter('''top_proba = 1''')
        
spdf = spdf.groupBy(['inn', 'kpp',]).agg(f.first(f.col('email_proba')).cast(DoubleType()).alias('email_proba'), 
                                         f.first(f.col('email_class')).cast(IntegerType()).alias('email_class'),
                                         f.first(f.col('asdayname')).alias('email_best_day'),
                                         f.first(f.col('ashour')).cast(IntegerType()).alias('email_best_hour'),
                                         f.lit('').alias('sms_best_day'),
                                         f.lit(None).cast(IntegerType()).alias('sms_best_hour'),
                                         f.lit(None).cast(DoubleType()).alias('sms_proba'),
                                         f.lit(None).cast(IntegerType()).alias('sms_class'),                                         
                                         f.first(f.col('fresh_user')).alias('fresh_user'),
                                         f.first(f.col('load_dt')).alias('load_dt'))        


# UPDATE: FOR EACH FOLLOWING CASES: Perform Full Oter Join with RESP_INN_KPP_PROBA
spdf = spdf.select(*[f.col(col).alias(col+'_') for col in spdf.columns])
table_name = 'RESP_INN_KPP_PROBA'
tmp_tbl = 'tmp_{}'.format(table_name)

spdf.registerTempTable(tmp_tbl)
hive.cacheTable(tmp_tbl)

alter_spdf = \
hive.sql(
'''
select 
 inn
,kpp
,email_proba
,email_class
,email_best_day
,email_best_hour

,sms_proba
,sms_class
,sms_best_day
,sms_best_hour

,fresh_user
,load_dt
from(
        select 
            t.*,
            row_number() over (partition by inn, 
                                            kpp,
                                            email_proba 
                                                         order by load_dt) rownum
        from(
                select
                    coalesce(p.inn, b.inn_) inn,
                    coalesce(p.kpp, b.kpp_) kpp,
                    
                    coalesce(b.email_proba_, p.email_proba) email_proba,
                    coalesce(b.email_class_, p.email_class) email_class,
                    cast(coalesce(Null, p.email_best_day) as string) email_best_day,
                    cast(coalesce(Null, p.email_best_hour) as int) email_best_hour,

                    
                    cast(coalesce(Null, p.sms_proba) as double) sms_proba,
                    cast(coalesce(Null, p.sms_class) as int) sms_class,
                    cast(coalesce('', p.sms_best_day) as string) sms_best_day,
                    cast(coalesce(Null, p.sms_best_hour) as int) sms_best_hour,
     
                    coalesce(b.fresh_user_, p.fresh_user) fresh_user,
                    coalesce(b.load_dt_, p.load_dt) load_dt
                from sbx_team_digitcamp.RESP_INN_KPP_PROBA p
                full outer join tmp_scores b on p.inn = b.inn_ and p.kpp = b.kpp_ 
                
        ) t
)
where rownum = 1
'''
)

table_name = 'RESP_INN_KPP_PROBA'
tmp_tbl = 'tmp_{}'.format(table_name)

alter_spdf.registerTempTable(tmp_tbl)

hive.sql('''drop table if exists sbx_team_digitcamp.{}__ purge'''.format(table_name))
hive.sql('''create table sbx_team_digitcamp.{}__ select * from {}'''.format(table_name,tmp_tbl))
hive.sql('''drop table if exists sbx_team_digitcamp.{} purge'''.format(table_name))
hive.sql('''alter table sbx_team_digitcamp.{}__ rename to sbx_team_digitcamp.{}'''.format(table_name, table_name))


# Export2Iskra
schema     = 'sbx_team_digitcamp'
table_name = 'RESP_INN_KPP_PROBA'

fin_df = hive.sql('''select * from {}.{} '''.format(schema,table_name))

cols = fin_df.columns

fin_df = fin_df.select([col.upper() for col in fin_df.columns])

typesmap={}
for column_name, column in fin_df.dtypes:
    if column == 'string':
        if 'INN' in column_name.upper() or 'KPP' in column_name.upper():
            typesmap[column_name] = 'VARCHAR(20)'
        elif 'commonSegmentoUID'.upper() in column_name.upper():
            typesmap[column_name] = 'VARCHAR(4000)'
        else:
            typesmap[column_name] = 'VARCHAR(900)'
    elif column == 'int':
        typesmap[column_name] = 'INTEGER'
    elif column == 'bigint':
        typesmap[column_name] = 'INTEGER'
    elif column == 'timestamp':
        typesmap[column_name] = 'TIMESTAMP'
    elif column == 'float' or column == 'double' or column == 'decimal(9,2)':
        typesmap[column_name] = 'FLOAT'        
    else:
        None  

cols = ', '.join([col + ' ' + typesmap[col] for col in fin_df.columns])

from connector import OracleDB

db = OracleDB('iskra4')
mode = 'overwrite'
fin_df \
    .write \
    .format('jdbc') \
    .mode(mode) \
    .option('url', 'jdbc:oracle:thin:@//'+db.dsn) \
    .option('user', db.user) \
    .option('password', db.password) \
    .option('dbtable', table_name) \
    .option('createTableColumnTypes', cols)\
    .option('driver', 'oracle.jdbc.driver.OracleDriver') \
    .save()
    
# Drop temp table
drop_tmp_table_list= ['tmp_preproc_resp_inn_kpp_camp','tmp_pre_ft_resp_inn_kpp_camp','tmp_fullres_resp_inn_kpp_camp']
for drop_t in drop_tmp_table_list:
    hive.sql('''drop table if exists sbx_team_digitcamp.{} purge'''.format(drop_t))
    time.sleep(1)

sp.exit()