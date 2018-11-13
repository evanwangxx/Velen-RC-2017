from sklearn.ensemble import AdaBoostClassifier
from sklearn.externals import joblib
from sklearn.metrics import confusion_matrix

def adaboost_train(X, y):

    adaboost = AdaBoostClassifier(base_estimator=None, n_estimators=50, learning_rate=1.0, algorithm='SAMME.R', random_state=None)

    joblib.dump(adaboost, '/Users/hongbo/PycharmProjects/DecisionTree/output/model/adaboost.m')

    model = adaboost.fit(X, y)

    return model

def adaboost_test(X, y, model):

    ans = model.predict(X)

    print "adaboost confusion matrix: "

    print confusion_matrix(y, ans)

    print "- - - - - - - - - - - - - - - - - - -"

def adaboost_pre(X, model):

    predict_result = model.predict(X)

    print "adaboost: ", predict_result

    return predict_result
