from sklearn import svm
from sklearn.externals import joblib
from sklearn.metrics import precision_recall_curve, confusion_matrix

## - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# svm linear model
def svm_linear(X, y):

    print "start train: clf_linear "

    clf_linear = svm.SVC(kernel='linear',
                         C=1.0,
                         gamma='auto',
                         probability = True,
                         class_weight=None,
                         verbose=False,
                         max_iter = 2000,
                         decision_function_shape=None,
                         random_state=None).fit(X, y)

    joblib.dump(clf_linear, '/Users/hongbo/PycharmProjects/DecisionTree/output/model/svm_linear.m')

    print clf_linear

    return clf_linear

def svm_linear_test(X_test, y_test, model):

    answer = model.predict(X_test)

    print "linear test confusion matrix: "

    print confusion_matrix(y_test, answer)

def svm_linear_predict(X, model):

    predict_result = model.predict(X)

    print "svm linear: ", predict_result

    return predict_result

## - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# svm rbf model
def svm_rbf(X, y):

    clf_rbf = svm.SVC(kernel = 'rbf',
                      C=1.0,
                      gamma='auto',
                      probability=True,
                      class_weight=None,
                      verbose=False,
                      max_iter = -1,
                      decision_function_shape=None,
                      random_state=None).fit(X, y)

    joblib.dump(clf_rbf, '/Users/hongbo/PycharmProjects/DecisionTree/output/model/svm_rbf.m')

    return clf_rbf

def svm_rbf_test(X_test, y_test, model):

    answer = model.predict(X_test)

    print " rbf test confusion matrix: "

    print confusion_matrix(y_test, answer)

    print "- - - - - - - - - - - - - - - - - - -"

def svm_rbf_predict(X, model):

    predict_result = model.predict(X)

    print "svm rbf: ", predict_result

    return predict_result

## - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# svm sigmoid model
def svm_sigmoid(X, y):

    clf_sigmoid = svm.SVC(kernel='sigmoid',
                          C=1.0,
                          gamma='auto',
                          coef0=0.0,
                          probability=False,
                          class_weight=None,
                          verbose=False,
                          max_iter = 2000,
                          decision_function_shape=None,
                          random_state=None).fit(X, y)

    joblib.dump(clf_sigmoid, '/Users/hongbo/PycharmProjects/DecisionTree/output/model/svm_sigmoid.m')

    return clf_sigmoid

def svm_sigmoid_test(X_test, y_test, model):

    answer = model.predict(X_test)

    print "sigmoid test confusion matrix: "

    print confusion_matrix(y_test, answer)

def svm_sigmoid_predict(X, model):

    predict_result = model.predict(X)

    print "svm sigmoid: ", predict_result

    return predict_result

## - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# svm poly model

def svm_poly(X, y):

    clf_poly = svm.SVC(kernel='poly',
                       C=1.0,
                       degree=3,
                       gamma='auto',
                       coef0=0.0,
                       probability=False,
                       class_weight=None,
                       verbose=False,
                       max_iter = 2000,
                       decision_function_shape=None,
                       random_state=None).fit(X, y)

    joblib.dump(clf_poly, '/Users/hongbo/PycharmProjects/DecisionTree/output/model/svm_poly.m')

    return clf_poly

def svm_poly_test(X_test, y_test, model):

    answer = model.predict(X_test)

    print " poly test confusion matrix: "

    print confusion_matrix(y_test, answer)

def svm_poly_predict(X, model):

    predict_result = model.predict(X)

    print "svm poly: ", predict_result

    return predict_result