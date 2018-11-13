import pandas as pd

import Data as dtm
import rules as rule

import desitionTree as destree
import svm as svm
import logistic as logistic
import random_forest as rft
import adaboost as adaboost

from sklearn.externals import joblib

# import data and clean data
pos_loc = '/Users/hongbo/PycharmProjects/DecisionTree/data/pos.txt'

neg_loc = '/Users/hongbo/PycharmProjects/DecisionTree/data/neg_%s.txt'

neg_list = ['20160610','20160710','20160810',
            '20160910','20161010','20161110',
            '20161210','20170110','20170210',
            '20170310','20170410','20170510']

all_data = dtm.import_data(pos_loc, neg_loc, neg_list, sample = 70000)

all_data = dtm.clean_null_variables(all_data)

all_data = dtm.calculate_time(all_data)

all_data = dtm.replace_by_zero('factor_poiid_in_highrisk_list', all_data)

all_data = dtm.province_clean(all_data)

X_train, X_test, y_train, y_test  = dtm.data_split_train_test(all_data)

X_train, X_test, y_train, y_test, col_names = dtm.imputation_data(X_train, X_test, y_train, y_test)

X_train = dtm.scale(X_train); X_test = dtm.scale(X_test)

# destree.output_plot(X_train, y_train)

# for i in range(5, 10):
#
#     clf = destree.train_tree(X_train, y_train, max_depth = i)
#
#     # clf = joblib.load("/Users/hongbo/PycharmProjects/DecisionTree/output/dtree.m")
#
#     important_features = destree.tree_stat(X_test, y_test, clf)

# destree.tree_viz(col_names, clf, 7)

# rule = rule.Rules(clf, important_features)

clf = destree.train_tree(X_train, y_train, max_depth = 6)

destree.tree_stat(X_test, y_test, clf)

svm_model = svm.svm_rbf(X_train, y_train)

svm.svm_rbf_test(X_test, y_test, svm_model)

logistic_model = logistic.logistic_train(X_train, y_train)

logistic.logistic_test(X_test, y_test, logistic_model)

rf = rft.rf_train(X_train, y_train)

rft.rf_test(X_test, y_test, rf)

ada = adaboost.adaboost_train(X_train, y_train)

adaboost.adaboost_test(X_test, y_test, ada)
