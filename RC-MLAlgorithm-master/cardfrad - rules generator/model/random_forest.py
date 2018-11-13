from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.metrics import confusion_matrix

def rf_train(X, y):

    rf = RandomForestClassifier()

    model = rf.fit(X, y)

    joblib.dump(model, '/Users/hongbo/PycharmProjects/DecisionTree/output/model/random_forest.m')

    return model

def rf_test(X, y, model):

    ans = model.predict(X)

    print "random forest confusion matrix: "

    print confusion_matrix(y, ans)

    print "- - - - - - - - - - - - - - - - - - -"

def rf_predict(X, model):

    predict_result = model.predict(X)

    print "random forest: ", predict_result

    return predict_result