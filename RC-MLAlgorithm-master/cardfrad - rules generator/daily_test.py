import Data as dt
import desitionTree as dst
from sklearn.externals import joblib

# date = ['20170401', '20170501', '20170601', '20170520', '20170521', '20170522','20170523','20170524']
result = dict()

date = ['20170515', '20170516', '20170517',
        '20170615', '20170616', '20170617']

for ti in date:

    print "date: ", ti

    loc = "/Users/hongbo/PycharmProjects/DecisionTree/data/test/test_userid_" + str(ti) + ".txt"

    test_data, userid = dt.import_data_test_daily_userid(loc)

    test_data = dst.imputation_data_daily_test(test_data)

    clf = joblib.load("/Users/hongbo/PycharmProjects/DecisionTree/output/dtree.m")

    predict = clf.predict(test_data)

    result[ti] = sum(predict)

    for i in range(0, len(predict)):

        if predict[i] == 1:

            print userid[i]

    print "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"

print result