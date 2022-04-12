from feature_importance import FeatureImportance
from sklearn.metrics import roc_auc_score, get_scorer
from sklearn.model_selection import KFold, StratifiedKFold, cross_validate
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


__all__ = ('ForwardPermutationFECV')


class ForwardPermutationFECV:
    
    def __init__(self, n_shuffle=3, n_folds=3, epsilon=0.005, metric='roc_auc', verbose=True,
                 early_stopping_rounds=20, folds = None, n_jobs=1):
        
        self.epsilon = epsilon
        self.n = n_shuffle
        self.early_stopping_rounds = early_stopping_rounds
        self.metric_name = metric
        self.metric = get_scorer(metric)
        if folds is None:
            self.folds = KFold(n_splits=n_folds, random_state=42)
        elif folds == 'StratifiedKFold':
            self.folds = StratifiedKFold(n_splits=n_folds, random_state=42)
        self.n_jobs = n_jobs
        self.verbose = verbose
        self.subsets_ = []


    def fit_transform(self, X, y, model):
        self.permutation_importance_df = None
        for i, (train_index, test_index) in enumerate(self.folds.split(X, y)):
            X_train, X_test = X.iloc[train_index], X.iloc[test_index]
            y_train, y_test = y.iloc[train_index], y.iloc[test_index]

            self.__fe = FeatureImportance(X_train, X_test, y_train, y_test, model, self.metric_name)
            if self.permutation_importance_df is None:
                self.permutation_importance_df = self.__fe.get_n_permutation_importance(self.n)
            else:
                self.permutation_importance_df = self.permutation_importance_df\
                    .merge(self.__fe.get_n_permutation_importance(self.n), on='features', suffixes=('', '_' + str(i)))
        self.permutation_importance_df['permutation_mean'] = self.permutation_importance_df.mean(axis=1)
        self.permutation_importance_df = self.permutation_importance_df.sort_values('permutation_mean', ascending=False)

        selected_features = []
        for i, col in enumerate(self.permutation_importance_df['features']):
            selected_features.append(col)
            if self.verbose:
                print('Fitting model on {0} features'.format(i + 1))
                # print('Features subset {0}'.format(selected_features))
            scores = cross_validate(model, X_train[selected_features], y_train, scoring=[self.metric_name], \
                        cv=self.folds, n_jobs=self.n_jobs, return_train_score=False)

            current_metric = scores['test_' + self.metric_name].mean()
            
            if self.verbose:
                print(self.metric_name + ' = {0}'.format(current_metric))
            self.subsets_.append({
                'score_' + self.metric_name: current_metric,
                'feature_names': list(selected_features)
            })

            if i > self.early_stopping_rounds and current_metric - self.subsets_[i - self.early_stopping_rounds] \
                    ['score_' + self.metric_name] < self.epsilon:
                break
            
        return selected_features[:-self.early_stopping_rounds]


    def plot_features_importance(self, filename='', palette = sns.color_palette('GnBu_d')):

        features_imp = self.permutation_importance_df.sort_values(by='permutation_mean', ascending=False).head(100)
        fig = plt.figure(figsize=(15,30))
        bp = sns.barplot(y=features_imp['features'], x=features_imp['permutation_mean'], orient='h', palette=palette)
        plt.show()
        fig.savefig(filename + '_permutation_mean' + '.jpg')
