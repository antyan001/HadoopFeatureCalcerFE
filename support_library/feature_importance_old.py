from multiprocessing import Pool, cpu_count
from joblib import Parallel, delayed
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import binarize
import numpy as np
import time
from sklearn.metrics import roc_auc_score, f1_score, accuracy_score, \
                            classification_report, precision_score, \
                            recall_score, roc_curve,confusion_matrix,\
                            precision_recall_curve, average_precision_score

__all__ = ('FeatureImportance')

class FeatureImportance(object):

    '''
    В классе реализованы методы исследования важности фич:
    Leave_one_out - поочередное удаление одного из признаков
    One_factor - построение однофакторных моделей
    PermutationImportance - поочередное перемещивание колонки признака
    '''
    def __init__(self, X_train, X_test, y_train, y_test, model, columns):
        '''
        Parameters
        ----------
        X_train: pandas.DataFrame
            Обучающий набор
        X_test: pandas.DataFrame
            Тестовый набор
        y_train: pandas.Series
            Целевая для обучающего набора
        y_test:  pandas.Series
            Целевая для тестового набора
        columns: list
            Список колонок для анализа
        model: object

        '''
        self.__X_train = X_train[columns]
        self.__X_test  = X_test[columns]
        self.__y_train = y_train
        self.__y_test  = y_test
        self.__model = model


    def predict(self, model, X_test, y_test, threshold=0.5):
        '''
        Получение метрики auc на тестовом наборе.

        Parameters
        ----------
        model: object
            Бинарный классификатор
        X_test: pandas.DataFrame
            Тестовый набор
        y_test: pandas.Series
            Целевая для тестового набора
        '''

        y_pred_proba = model.predict_proba(X_test)[:, 1]
        y_pred = binarize(y_pred_proba.reshape(-1, 1), threshold = threshold).reshape(-1)

        auc = roc_auc_score(y_test.astype(int), y_pred_proba)
        precision = precision_score(y_test.astype(int), y_pred)
        recall = recall_score(y_test.astype(int), y_pred)
        TN, FP, FN, TP = confusion_matrix(y_test, y_pred).ravel()

        print('AUC:%.5f, Precision:%.5f, Recall:%.5f, True:%d of %d, Total:%d, Tr:%.2f' %
                          (auc, precision, recall,
                           TP, TP + FP, TP + FN,
                           threshold), flush = True)

        return auc


    def get_model_auc(self, X_train, X_test, y_train, y_test, threshold=0.5):
        '''
        Обучение модели и получение метрики auc на тестовом наборе.

        Parameters
        ----------
        threshold: float
            Порог для разделения классов
        '''

        self.__model.fit(X_train, y_train)
        auc = self.predict(self.__model, X_test, y_test)
        return auc


    def _leave_one_out(self, args):
        '''
        Метод, который вычисляет AUC без одной фичи. Не должен вызываться напрямую

        Parameters
        ----------
        args: list
            Массив из двух значений: feature, full_model_auc
            feature - имя фичи, без которой нужно вычислить AUC
            full_model_auc - AUC модели со всеми фичами

        Return value
        ----------
        [feature, full_model_auc - auc]: list
            feature -  имя фичи, без которой вычислен AUC
            full_model_auc - auc исходной модели - auc модели без текущего признака.
        '''
        feature, full_model_auc = args
        auc = self.get_model_auc(self.__X_train.drop(feature, axis = 1),
                              self.__X_test.drop(feature, axis = 1),
                              self.__y_train, self.__y_test)
        return [feature, full_model_auc - auc]


    def get_leave_one_out(self, n_jobs=1, verbose=0):
        '''
        Подбор признаков Leave-One-Out - поочередное удаление одного из признаков.

        Parameters
        ----------
        n_jobs: int
            Количество потоков для вычисления.
        verbose: int
            Если значение больше 0, то будет выводиться отладочная информация
        Return value
        ----------
        feature_importance: pandas.DataFrame
            Содержит столбцы [Признак, leave_one_out_auc]
                leave_one_out_auc = full_model_auc-auc
                (auc исходной модели - auc модели без текущего признака.)
            отрицательное значение - признак ухудшает модель
            положительное значение - признак улучшает модель
        '''
        full_model_auc = self.get_model_auc(self.__X_train,self.__X_test,
                                     self.__y_train, self.__y_test)
        feature_importance = pd.DataFrame()

        print(self.__X_train.columns)
        results = Parallel(n_jobs=n_jobs, verbose=verbose) (map(delayed(self._leave_one_out), [(col, full_model_auc)
                 for col in self.__X_train.columns]))


        feature_importance = feature_importance.append(results)

        feature_importance.columns=['features', 'leave_one_out_auc']
        feature_importance = feature_importance.sort_values(by='leave_one_out_auc', ascending = False).reset_index(drop = True)
        return feature_importance


    def get_one_factor_importance(self):
        '''
        Построение однофакторных моделей - построение моделей от каждого признака.

        Return value
        ----------
        feature_importance: pandas.DataFrame
            Содержит столбцы [Признак, one_fact_auc]
                one_fact_auc - auc для однофакторной модели.
        '''
        feature_importance = pd.DataFrame()
        for feature in self.__X_train.columns:
            print('-' * 80)
            print('Feature:', feature, flush = True)
            auc = self.get_model_auc(self.__X_train[[feature]], self.__X_test[[feature]], self.__y_train, self.__y_test)
            feature_importance = feature_importance.append([[feature, auc]])

        feature_importance.columns=['features', 'one_fact_auc']
        feature_importance = feature_importance.sort_values(by='one_fact_auc', ascending = False).reset_index(drop = True)
        return feature_importance


    def get_permutation_importance(self, model=None):
        '''
        Подбор признаков PermutationImportance - поочередная выборка признака,
        его перемещивание и возвращение в датафрейм. Расчет на новом наборе метрики auc.
`
        Parameters
        ----------
        model: object
            Бинарный классификатор
            если model = None - обучаем переданную в конструктор класса модель,
            иначе используем предварительно обученную и переданную в данную функцию

        Return value
        ----------
        feature_importance: pandas.DataFrame
            Содержит столбцы [Признак, permutation_auc]
                permutation_auc = full_model_auc - auc
                (auc исходной модели - auc модели текущим перемещанным признаком.)
            отрицательное значение - признак ухудшает модель
            положительное значение - признак улучшает модель
        '''
        if not model:
            model = self.__model.fit(self.__X_train, self.__y_train)
        full_model_auc = self.predict(model, self.__X_test, self.__y_test)
        feature_importance = pd.DataFrame()

        for feature in self.__X_train.columns:
            F_test = self.__X_test[feature].copy()

            np.random.shuffle(self.__X_test[feature].values)

            print('-' * 80)
            print('Feature:', feature, flush = True)
            auc = self.predict(model, self.__X_test, self.__y_test)
            auc = full_model_auc - auc
            feature_importance = feature_importance.append([[feature, auc]])

            self.__X_test[feature] = F_test
        feature_importance.columns=['features', 'permutation_auc']
        feature_importance = feature_importance.sort_values(by='permutation_auc', 
                                                            ascending = False).reset_index(drop = True)
        
        return feature_importance


    def get_n_permutation_importance(self, n=2, model=None):
        '''
        Подбор признаков PermutationImportance - поочередная выборка признака,
        его перемещивание и возвращение в датафрейм. Расчет на новом наборе метрики auc.
    `
        Parameters
        ----------
        n: int
            Количество перемешиваний колонки и предсказания на тестовой выборке
        model: object
            Бинарный классификатор
            если model = None - обучаем переданную в конструктор класса модель,
            иначе используем предварительно обученную и переданную в данную функцию

        Return value
        ----------
        feature_importance: pandas.DataFrame
            Содержит столбцы [Признак, permutation_auc]
                permutation_auc = full_model_auc - auc
                (auc исходной модели - auc модели текущим перемещанным признаком.)
            отрицательное значение - признак ухудшает модель
            положительное значение - признак улучшает модель
        '''
        if not model:
            model = self.__model.fit(self.__X_train, self.__y_train)
        full_model_auc = self.predict(model, self.__X_test, self.__y_test)
        feature_importance = pd.DataFrame()

        for feature in self.__X_train.columns:
            print('-' * 80)
            print('Feature:', feature, flush = True)

            original_feature = self.__X_test[feature].copy()

            auc_scores = []
            for i in range(n):
                np.random.shuffle(self.__X_test[feature].values)
                auc_scores.append(self.predict(model, self.__X_test, self.__y_test))

            auc = full_model_auc - np.max(auc_scores) \
                if np.abs(full_model_auc - np.max(auc_scores)) > \
                np.abs(full_model_auc - np.min(auc_scores)) \
                else full_model_auc - np.min(auc_scores)

            # auc = full_model_auc - np.mean(auc_sctopores)
            feature_importance = feature_importance.append([[feature, auc]])

            self.__X_test[feature] = original_feature
        feature_importance.columns=['features', 'permutation_auc']

        return feature_importance


    def plot_features_importance(self, features_imp, method, filename='', palette = sns.color_palette('GnBu_d')):
        '''
        Построение графика важности признаков.
        features_imp - датафрейм Признак, Метрика
        method - 'permutation', 'one_fact', 'leave_one_out'
        filename - имя файла
        palette - seaborn.color_palette для окраски графика
        '''
        features_imp = features_imp.sort_values(by=method + '_auc', ascending=False).head(100)
        fig = plt.figure(figsize=(15,30))
        bp = sns.barplot(y=features_imp['features'], x=features_imp[method + '_auc'], orient='h', palette=palette)
        plt.show()
        if method == 'one_fact':
            bp.set(xlim=(0.5, 1.0))
        fig.savefig(filename + '_' + method + '.jpg')
