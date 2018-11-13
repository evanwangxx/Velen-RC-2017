from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
from sklearn.metrics import confusion_matrix

def logistic_train(X, y):

    lr = LogisticRegression(penalty='l2',
                            dual=False,
                            tol=0.0001,
                            C = 0.01,
                            fit_intercept=True,
                            intercept_scaling=1,
                            class_weight=None,
                            random_state=None,
                            solver='sag',
                            max_iter = 5000,
                            multi_class='ovr',
                            verbose=0,
                            warm_start=False,
                            n_jobs=1)
    model = lr.fit(X, y)

    joblib.dump(model, '/Users/hongbo/PycharmProjects/DecisionTree/output/model/logistic.m')

    return model

def logistic_test(X_test, y_test, model):

    answer = model.predict(X_test)

    print " logistic confusion matrix: "

    print confusion_matrix(y_test, answer)

    print "- - - - - - - - - - - - - - - - - - -"

def logistic_pred(X, model):

    predict_result = model.predict(X)

    print "logistic: ", predict_result

    return predict_result