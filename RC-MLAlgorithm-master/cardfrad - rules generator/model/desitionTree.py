### This file is create to train a Decision Tree Model

## import packages
import Data as df
import os
import pydotplus
import numpy as np
import matplotlib.pyplot as plt
import sklearn
from sklearn import tree
from sklearn.metrics import precision_recall_curve, confusion_matrix
from sklearn.externals import joblib
from pandas import DataFrame

## - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# plot CV curve
def plot_learning_curve(estimator, X, y, ylim=(0.8, 1.1), cv=3,
                        n_jobs=1, train_sizes=np.linspace(.1, 1.0, 5),
                        scoring=None):

    plt.title("Learning curves for %s" % type(estimator).__name__)

    plt.ylim(*ylim); plt.grid()

    plt.xlabel("Training examples")

    plt.ylabel("Score")

    train_sizes, train_scores, validation_scores = sklearn.model_selection.learning_curve(
        estimator, X, y, cv=cv, n_jobs=n_jobs, train_sizes=train_sizes,
        scoring=scoring)

    train_scores_mean = np.mean(train_scores, axis=1)

    validation_scores_mean = np.mean(validation_scores, axis=1)

    plt.plot(train_sizes, train_scores_mean, 'o-', color="r",
             label="Training score")

    plt.plot(train_sizes, validation_scores_mean, 'o-', color="g",
             label="Cross-validation score")

    plt.legend(loc="best")

    plt.savefig("/Users/hongbo/PycharmProjects/DecisionTree/output/fig1.png")  ## modify your location

    print("Best validation score: {:.4f}".format(validation_scores_mean[-1]))

# plot cross-validation curve
def plot_validation_curve(estimator, X, y, param_name, param_range,
                          ylim=(0.9, 1.05), cv=3, n_jobs=1, scoring=None):

    estimator_name = type(estimator).__name__

    plt.title("Validation curves for %s on %s"
              % (param_name, estimator_name))

    plt.ylim(*ylim)
    
    plt.grid()

    plt.xlim(min(param_range), max(param_range))

    plt.xlabel(param_name)

    plt.ylabel("Score")

    train_scores, test_scores = sklearn.model_selection.validation_curve(
        estimator, X, y, param_name, param_range,
        cv=cv, n_jobs=n_jobs, scoring=scoring)

    train_scores_mean = np.mean(train_scores, axis=1)

    test_scores_mean = np.mean(test_scores, axis=1)

    plt.semilogx(param_range, train_scores_mean, 'o-', color="r",
                 label="Training score")

    plt.semilogx(param_range, test_scores_mean, 'o-', color="g",
                 label="Cross-validation score")

    plt.legend(loc="best")

    plt.savefig("/Users/hongbo/PycharmProjects/DecisionTree/output/fig2.png")  ## modify own location

    print("Best test score: {:.4f}".format(test_scores_mean[-1]))

def output_plot(X_train, y_train):

    clf = tree.DecisionTreeClassifier()  # maximum depth = 8

    clf = clf.fit(X_train, y_train)

    plot_learning_curve(clf, X_train, y_train, scoring='roc_auc')

    param_name = 'max_depth'

    param_range = [5,6,7,8,9,10,11,12,13,14,15,16,17]

    plot_validation_curve(clf, X_train, y_train, param_name, param_range, scoring='roc_auc')

## - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Model training;

def train_tree(X_train, y_train, max_depth = None):

    clf = tree.DecisionTreeClassifier(max_depth = max_depth)   # maximum depth = 8

    clf = clf.fit(X_train, y_train)

    joblib.dump(clf, '/Users/hongbo/PycharmProjects/DecisionTree/output/model/dtree.m')       # save the model we trained

    return clf

def tree_stat(X_test, y_test, clf):

    answer = clf.predict(X_test)

    print "DT confusion matrix: "

    print confusion_matrix(y_test, answer)

    print "- - - - - - - - - - - - - - - - - - -"

def tree_stat_more(X_test, y_test, clf):

    precision, recall, thresholds = precision_recall_curve(y_test, clf.predict_proba(X_test)[:, 1])

    print "precision: ", precision

    print "recall: ", recall

    print "thresholds: ", thresholds

    variable_names = X_test.columns.values

    result_table = {"importances" : clf.feature_importances_,
                    "variable": variable_names}

    result_features_presort = DataFrame(data = result_table)

    result_features = result_features_presort.sort_values(by = 'importances',
                                                  ascending = False)

    print " - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"

    print "top 20 important features"

    print result_features.head(20)

    print " - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"

    return result_features

## - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Tree Visualization

def tree_viz(col_names, clf, depth):

    with open("/Users/hongbo/PycharmProjects/DecisionTree/output/iris.dot", 'w') as f:

        f = tree.export_graphviz(clf, out_file=f)

    os.unlink('/Users/hongbo/PycharmProjects/DecisionTree/output/iris.dot')

    class_name = ["safe", "risky"]

    dot_data = tree.export_graphviz(clf, out_file = None,
                                    feature_names = col_names,  # X.columns.values
                                    class_names = class_name,
                                    filled = True,
                                    rounded = True,
                                    proportion = False,
                                    rotate = False)

    graph = pydotplus.graph_from_dot_data(dot_data)

    graph.write_pdf("/Users/hongbo/PycharmProjects/DecisionTree/output/desition_tree_" + str(depth) + ".pdf")

