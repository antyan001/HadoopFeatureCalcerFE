import re, os, time, sys, datetime, csv
import gc
import itertools
from tqdm import tqdm
from copy import deepcopy
import numpy as np
import pandas as pd
from .label_encoder import SoftLabelEncoder
from scipy.stats import ks_2samp
from sklearn.preprocessing import PolynomialFeatures
from sklearn.preprocessing import scale, MinMaxScaler, Imputer
from sklearn.externals import joblib

import seaborn as sns; sns.set()
import matplotlib.pyplot as plt
from matplotlib.ticker import NullFormatter

__all__ = ('Featureng')

class Featureng(object):
    
    @classmethod
    def get_path(self, folder, file):
        return os.path.join(folder, file)
    
    @staticmethod
    def strtype(var):
        try:
            if int(var)==float(var):
                return 'int'
        except:
            try:
                float(var)
                return 'float'
            except:
                return 'str'


    @classmethod
    def add_noise(self, df, noise_level):
        if df is not None:
            for col in df.columns:
                if df[col].dtypes != 'O':
                    df[col]=df[col] * (1 + noise_level * np.random.randn(len(df)))
        return df
    
    @classmethod
    def target_encode_df_join(        
                                      self,
                                      trn_df=None, 
                                      tst_df=None, 
                                      target=None, 
                                      cat_features_lst=None,
                                      min_samples_leaf=100, 
                                      exp_norm=100,
                                      noise_level=0.01,
                                      save_dict = False
                              ):
        """
        Smoothing is computed like in the following paper by Daniele Micci-Barreca
        https://kaggle2.blob.core.windows.net/forum-message-attachments/225952/7441/high%20cardinality%20categoricals.pdf
        trn_series : training categorical feature as a pd.Series
        tst_series : test categorical feature as a pd.Series
        target : target data as a pd.Series
        min_samples_leaf (int) : minimum samples to take category average into account
        smoothing (int) : smoothing effect to balance categorical average vs prior  
        """ 

        if tst_df is not None:
            assert len(trn_df.columns) == len(tst_df.columns)
            assert len(list(set(trn_df.columns) - set(tst_df.columns))) == 0

        target_dict={}
        
        for col in cat_features_lst:

            trn_series=trn_df[col]
            if tst_df is not None:
                tst_series=tst_df[col]
            else:
                tst_series=None

            temp = pd.concat([trn_series, target], axis=1)

            # Compute target mean
            averages = temp.groupby(by=trn_series.name)[target.name].agg([('mean','mean'),
                                                                          ('count','count'),
                                                                          ('std', lambda x: np.std(x,ddof=0))]).reset_index()

            # Compute smoothing
            smoothing = 1. / (1. + np.exp(-(averages['count'] - min_samples_leaf) / exp_norm))
            # Apply average function to all target data
            prior     = target.mean()
            prior_std = target.std()    

            # The bigger the count the less full_avg is taken into account
            averages['_avg_mean'] = prior * (1 - smoothing) + averages['mean'] * smoothing
            averages['_avg_std']  = prior_std * (1 - smoothing) + averages['std'] * smoothing
            averages.drop(["mean", "count", "std"], axis=1, inplace=True)

            target_dict.update({col:averages.set_index(trn_series.name).to_dict()})

            # Apply averages to trn and tst series
            ft_trn_series = pd.merge(
                trn_series.to_frame(trn_series.name),
                averages,
                on=trn_series.name,
                how='left')[['_avg_mean','_avg_std']].rename(columns={
                                                                      '_avg_mean': trn_series.name+'_avg_mean',
                                                                      '_avg_std' : trn_series.name+'_avg_std'})
            
            ft_trn_series[trn_series.name+'_avg_mean'].fillna(prior)
            ft_trn_series[trn_series.name+'_avg_std'].fillna(prior_std)
 
            # pd.merge does not keep the index so restore it
            ft_trn_series.index = trn_series.index 
        
            if tst_series is not None:
                ft_tst_series = pd.merge(
                    tst_series.to_frame(tst_series.name),
                    averages,
                    on=tst_series.name,
                    how='left')[['_avg_mean','_avg_std']].rename(columns={
                                                                          '_avg_mean': trn_series.name+'_avg_mean',
                                                                          '_avg_std' : trn_series.name+'_avg_std'})
                ft_tst_series[trn_series.name+'_avg_mean'].fillna(prior)
                ft_tst_series[trn_series.name+'_avg_std'].fillna(prior_std)
                # pd.merge does not keep the index so restore it
                ft_tst_series.index = tst_series.index
            else:
                ft_tst_series=None


            ft_trn_series = self.add_noise(ft_trn_series, noise_level)

            trn_df=trn_df.join(ft_trn_series)

            if ft_tst_series is not None:
                ft_tst_serie = self.add_noise(ft_tst_series, noise_level)
                tst_df=tst_df.join(ft_tst_series)

            #trn_df.drop([col], axis = 1, inplace=True)

            #if tst_df is not None:
            #    tst_df.drop([col], axis = 1, inplace=True)

        if save_dict:
            joblib.dump(target_dict, self.get_path('pkl_store', 'target_enc_dict.pkl'))
        
        return trn_df, tst_df
    
    @classmethod
    def addpolyfeatures(self, df, cattopoly):
        
        pf = PolynomialFeatures(3,interaction_only=True)
        pf_df=pd.DataFrame(pf.fit_transform(df[cattopoly].fillna(df[cattopoly].mean())), 
                           columns=pf.get_feature_names(cattopoly), index=df.index)  

        pf_df.drop(["1"], axis=1, inplace=True)
        pf_df.drop(cattopoly, axis=1, inplace=True)
        df=df.join(pf_df)          

        return df  
    
    @classmethod
    def labeleng(self, trn, tst, save_dict):
        label_encoders = {}
        for col in trn.dtypes[trn.dtypes == 'O'].index:
            le = SoftLabelEncoder()
            trn.loc[:,col]       = trn.loc[:,col].apply(lambda _x: str(_x) \
                                                        if self.strtype(var=_x) != 'str' else _x)        
            trn[col] = le.fit_transform(trn.loc[:,col].fillna('NaN'))
            label_encoders[col] = le
            
        for col in tst.dtypes[tst.dtypes == 'O'].index:
            tst[col] = label_encoders[col].transform(tst[col].fillna('NaN'))        

        if save_dict:
            joblib.dump(label_encoders, self.get_path('pkl_store', 'le_dict_full.pkl'))
            
        return trn, tst

    @classmethod
    def gen_WoE_IV (self, trn_df, tst_df, y_trn, feature_lst, noise_level, save_dict):
        '''
        lst=[]
        for i in range(df[feature].nunique()):
            val = list(df[feature].unique())[i]
            lst.append([val,               
                        df[(df[feature] == val) & (df[target] == 0)].count()[feature],  
                        df[(df[feature] == val) & (df[target] == 1)].count()[feature]]) 

        data = pd.DataFrame(lst, columns=[feature, 'Unsold', 'Sold'])

        data['WoE'] = np.log( (data['Sold'] / data['Sold'].sum()) / (data['Unsold'] / data['Unsold'].sum()) )

        data = data.replace({'WoE': {np.inf: 0, -np.inf: 0}})

        data['IV'] = data['WoE'] * (data['Sold'] / data['Sold'].sum() - data['Unsold'] / data['Unsold'].sum())   
        '''
        
        tmp=trn_df.join(y_trn)
        target=y_trn.name
        woe_dict={}
        
        for feature in feature_lst:
              
            temp=tmp.groupby(feature)[target].agg([('ratio_1', lambda x: x.sum()/x.count()),
                                                   ('ratio_0', lambda x: (x.count() - x.sum())/x.count())]).reset_index()

            #temp['ratio_1']=temp['sum_1'] / temp['sum_1'].sum()
            #temp['ratio_0']=temp['sum_0'] / temp['sum_0'].sum()
            temp['WoE']=np.log(temp['ratio_1'] / temp['ratio_0'])
            
            temp = temp.replace({'WoE': {np.inf: 0, -np.inf: 0}})
            
            temp['IV']=temp['WoE'] * (temp['ratio_1'] - temp['ratio_0'])
            temp.drop(["ratio_1", "ratio_0"], axis=1, inplace=True)
            
            temp = self.add_noise(temp, noise_level)
            
            woe_dict.update({feature:temp.set_index(feature).to_dict()})
                       
            trn_df = pd.merge(trn_df, temp[[feature,'WoE','IV']], 
                              on=feature, how='left').rename(columns={'WoE': feature+'_WoE',
                                                                       'IV': feature+'_IV'})
            
            trn_df[feature+'_WoE'].fillna(temp['WoE'].mean())
            trn_df[feature+'_IV'].fillna(temp['IV'].mean())
            
            tst_df = pd.merge(tst_df, temp[[feature,'WoE','IV']], 
                              on=feature, how='left').rename(columns={'WoE': feature+'_WoE',
                                                                       'IV': feature+'_IV'})            
            
            tst_df[feature+'_WoE'].fillna(temp['WoE'].mean())
            tst_df[feature+'_IV'].fillna(temp['IV'].mean())

        if save_dict:
            joblib.dump(woe_dict, self.get_path('pkl_store', 'woe_dict.pkl'))           
            
        return  trn_df, tst_df    
    
 
    @classmethod
    def features_endineering(self, X_train, X_test, y_train, cat_features, cattopoly, save_dict):

        #cat_features_lst=list(X_train.dtypes[X_train.dtypes == 'O'].index)
        
        # make mean/std target encoding over cat_features list
        trn, tst = self.target_encode_df_join(   
                                                 X_train, 
                                                 X_test, 
                                                 target=y_train, 
                                                 cat_features_lst=cat_features,
                                                 min_samples_leaf=100,
                                                 exp_norm=100,
                                                 noise_level=0.01,
                                                 save_dict= save_dict
                                             )  
        
        # make Weights of Evidence + local Information Value encoding over cat_features list
        trn, tst = self.gen_WoE_IV (trn, tst, y_train, cat_features, 
                                    noise_level=0.01, save_dict = save_dict) 
        # make label encoging
        trn, tst = self.labeleng(trn, tst, save_dict)
        
        # produce polynomial features (3 level interaction only)
        trn      = self.addpolyfeatures(trn, cattopoly) 
        tst      = self.addpolyfeatures(tst, cattopoly) 

        X_train, X_test = trn, tst

        return X_train, X_test 
    
    @classmethod
    def features_enc_from_dict(self, trn, cat_features, cattopoly):

        target_dict = joblib.load(self.get_path('pkl_store', 'target_enc_dict.pkl'))
        le_dict     = joblib.load(self.get_path('pkl_store', 'le_dict.pkl'))
        woe_dict    = joblib.load(self.get_path('pkl_store', 'woe_dict.pkl'))

        for col in cat_features:            
            trn[col+'_avg_mean'] = trn[col].map(target_dict[col]['_avg_mean'])
            trn[col+'_avg_std']  = trn[col].map(target_dict[col]['_avg_std'])        
        
        for col in cat_features:
            trn[col+'_WoE']      = trn[col].map(woe_dict[col]['WoE'])
            trn[col+'_IV']       = trn[col].map(woe_dict[col]['IV'])
            
        for col in cat_features:          
            trn.loc[:,col]       = trn.loc[:,col].apply(lambda _x: str(_x) \
                                                        if self.strtype(var=_x) != 'str' else _x)   
            trn[col]             = le_dict[col].transform(trn[col])

        # produce polynomial features (3 level interaction only)
        trn      = self.addpolyfeatures(trn, cattopoly) 

        return trn


    @classmethod
    def feature_scale_trans (self, df_imp):

        # Columns to drop because there is no variation in training set
        zero_std_cols = df_imp.columns[df_imp.std() == 0]
        df_imp.drop(zero_std_cols, axis=1, inplace=True)
        print('Removed {} constant columns'.format(len(zero_std_cols)))

        # Removing duplicate columns

        # colsToRemove = []
        # colsScaned = []
        # dupList = {}
        # columns = df.columns
        # for i in range(len(columns)-1):
        #     v = df[columns[i]].values
        #     dupCols = []
        #     for j in range(i+1,len(columns)):
        #         if np.array_equal(v, df[columns[j]].values):
        #             colsToRemove.append(columns[j])
        #             if columns[j] not in colsScaned:
        #                 dupCols.append(columns[j]) 
        #                 colsScaned.append(columns[j])
        #                 dupList[columns[i]] = dupCols
        # colsToRemove = list(set(colsToRemove))
        # df.drop(colsToRemove, axis=1, inplace=True)
        # print('Dropped {} duplicate columns'.format(len(colsToRemove)))

        # Go through the columns one at a time (can't do it all at once for this dataset)
        df_all = df_imp.copy()
        for col in df_imp.columns:

            # Detect outliers in this column
            data = df_imp[col].values.copy()
            data_mean, data_std = np.mean(data), np.std(data, ddof=0)
            #print('{} --> mean: {} std: {}'.format(col, data_mean, data_std))
            cut_off = data_std * 3
            lower, upper = data_mean - cut_off, data_mean + cut_off
            outliers = [x for x in data if x < lower or x > upper]

            # If there are crazy high values, do a log-transform
            if len(outliers) > 0:
                non_zero_idx = (data != 0) & (data > 0) 
                df_imp.loc[non_zero_idx, col] = np.log(data[non_zero_idx])
                df_imp.replace({col: {np.inf: 0, -np.inf: 0}}, inplace=True)

                df_all.loc[non_zero_idx, col] = np.log(data[non_zero_idx])
                df_all.replace({col: {np.inf: 0, -np.inf: 0}}, inplace=True)        

            # Scale non-zero column values
            nonzero_rows = df_imp[col] != 0
            if np.any(nonzero_rows):
                df_imp.loc[nonzero_rows, col] = scale(df_imp.loc[nonzero_rows, col])

            # Scale all column values
            df_all.loc[:, col] = scale(df_all.loc[:, col])
            gc.collect()

        return df_imp, df_all
    
    @classmethod
    def get_diff_columns(self, train_df, test_df, show_plots=True, show_all=False, 
                         threshold=0.1, kde=False):
        """Use KS to estimate columns where distributions differ a lot from each other"""

        # Find the columns where the distributions are very different
        diff_data = []
        for col in train_df.columns:
            statistic, pvalue = ks_2samp(
                train_df[col].values, 
                test_df[col].values
            )
            if pvalue <= 0.05 and np.abs(statistic) >= threshold:
                diff_data.append({'feature': col, 'p': np.round(pvalue, 5), 'statistic': np.round(np.abs(statistic), 3)})

        # Put the differences into a dataframe
        diff_df = pd.DataFrame(diff_data)
        diff_df.sort_values(by='statistic', ascending=False, inplace=True)

        if show_plots:
            # Let us see the distributions of these columns to confirm they are indeed different
            n_cols = 3
            if show_all:
                n_rows = int(len(diff_df) / 3)
            else:
                n_rows = 7
            _, axes = plt.subplots(n_rows, n_cols, figsize=(20, 3*n_rows))
            axes = [x for l in axes for x in l]

            # Create plots
            for i, (_, row) in enumerate(diff_df.iterrows()):
                if i >= len(axes):
                    break
                if not kde:    
                    extreme = np.max(np.abs(train_df[row.feature].tolist() + test_df[row.feature].tolist()))
                    train_df.loc[:, row.feature].hist(
                        ax=axes[i], alpha=0.5, label='Train', density=True,
                        bins=np.arange(-extreme, extreme, 0.05)
                    )
                    test_df.loc[:, row.feature].hist(
                        ax=axes[i], alpha=0.5, label='Test', density=True,
                        bins=np.arange(-extreme, extreme, 0.05)
                    )
                    axes[i].legend()
                else:
                    sns.distplot(train_df.loc[:, row.feature], label='Train', 
                                 hist=False, kde=True, norm_hist=True, ax=axes[i])
                    sns.distplot(test_df.loc[:, row.feature], label='Test', 
                                 hist=False, kde=True, norm_hist=True, ax=axes[i])

                axes[i].set_title('Statistic = {}, p = {}'.format(row.statistic, row.p))
                axes[i].set_xlabel('{}'.format(row.feature))


            plt.tight_layout()
            plt.show()

        return diff_df







        
    