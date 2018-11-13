### Use this file to load, clean data
## import data
## calculate time difference
## Clean provinces: One-hot method
## replace Null items in greyList

import math
import pandas as pd
from pandas import DataFrame
from sklearn.preprocessing import Imputer
from sklearn.model_selection import train_test_split
from sklearn import preprocessing

## ************************************************
# import Data and combine data
# pos: risky && zero: normal

def import_data(pos_loc, neg_loc, neg_list, sample):

    print " ### Check imported data ###"

    neg_path = [pd.read_table(neg_loc % i) for i in neg_list]

    negative_data = pd.concat(neg_path)

    negative_data["label"] = 0

    negative_data = negative_data.sample(n = sample,
                                         frac = None,
                                         replace = False,
                                         weights = None,
                                         random_state = None,
                                         axis = None)

    positive_data = pd.read_table(pos_loc)

    positive_data["label"] = +1

    frames = [positive_data, negative_data]

    all_data = pd.concat(frames)

    # delete useless
    del all_data['factor_rule_quickpay_gray_dealid']
    del all_data['factor_imsi_in_sibship_blacklist']

    print "column check: ", positive_data.columns.values == negative_data.columns.values

    print 'positive data dimension: ', positive_data.shape, '| data type: ', type(positive_data)

    print 'negative data dimension: ', negative_data.shape, '| data type: ', type(negative_data)

    print "all data dimension: ", all_data.shape

    return all_data

## ************************************************
# calculate time difference:
# current_time and register_time || current_time and quickSign_time

def calculate_time(all_data):

    def calculate_time_difference(x, y):
        if y > 0:

            return abs(y - x)

        else:

            return None                                           # in case of one of columns is empty

    all_data['time_difference_between_current_and_quickSign'] = map(lambda x,y:calculate_time_difference(x,y),
                                                                    all_data['factor_currenttime'],
                                                                    all_data['factor_quicksign_time_medis_from_null_0'])

    all_data['time_difference_between_current_and_regTime'] = map(lambda x,y:calculate_time_difference(x,y),
                                                                    all_data['factor_currenttime'],
                                                                    all_data['factor_regtime'])

    # delete original columns since they are useless right now
    del all_data['factor_currenttime']

    del all_data['factor_regtime']

    del all_data['factor_quicksign_time_medis_from_null_0']

    return all_data

## ************************************************
'''
factor_bankmobile_diff_notifymobile
factor_mobile_diff_notifymobile
factor_poi_ip_location_mobile_same_provin
factor_poi_ip_location_mobile_nil
factor_trust_user_subip_score_v2
factor_trust_user_uuid_score_v2
factor_userid_login_too_many_uuid_30d
factor_user_uuid_login_success_user_cnts_7d_10
'''

def clean_null_variables(data):

    print ''' ** start to clean: 
                                factor_bankmobile_diff_notifymobile
                                factor_mobile_diff_notifymobile
                                factor_poi_ip_location_mobile_same_provin
                                factor_poi_ip_location_mobile_nil
                                factor_trust_user_subip_score_v2
                                factor_trust_user_uuid_score_v2
                                factor_userid_login_too_many_uuid_30d
                                factor_user_uuid_login_success_user_cnts_7d_10
          '''

    data['factor_bankmobile_diff_notifymobile'].fillna(False)

    data['factor_mobile_diff_notifymobile'].fillna(False)     ## warn

    data['factor_poi_ip_location_mobile_same_provin'].fillna(False)

    data['factor_poi_ip_location_mobile_nil'].fillna(True)

    data['factor_trust_user_subip_score_v2'].fillna(0)

    data['factor_trust_user_uuid_score_v2'].fillna(0)

    data['factor_userid_login_too_many_uuid_30d'].fillna(1)   ## warn

    data['factor_user_uuid_login_success_user_cnts_7d_10'].fillna(1)

    return data

## ************************************************
# Clean greyList
# Replace Null with int 0

def replace_by_zero(name, all_data):

    all_data_nrow = all_data.columns.get_loc(name)  #'factor_poiid_in_highrisk_list'

    for index in range(1, all_data.shape[0]):

        # print "index is: ", index, "|", math.isnan(all_data.iloc[index, all_data_nrow])

        if math.isnan(all_data.iloc[index, all_data_nrow]):

            all_data.iloc[index, all_data_nrow] = 0

    print "Data manipulation: Replace Done"

    return all_data

## ************************************************
# Province: change province from Han-String to int
# Clean provinces
# One-hot method

def province_clean(all_data):

    all_province = {'\xe6\xb2\xb3\xe5\x8d\x97\xe7\x9c\x81',
                    '\xe7\xa6\x8f\xe5\xbb\xba\xe7\x9c\x81',
                    '\xe9\x99\x95\xe8\xa5\xbf\xe7\x9c\x81',
                    '\xe5\x9b\x9b\xe5\xb7\x9d\xe7\x9c\x81',
                    '\xe8\xbe\xbd\xe5\xae\x81\xe7\x9c\x81',
                    '\xe6\xb1\x9f\xe8\x8b\x8f\xe7\x9c\x81',
                    '\xe5\xb9\xbf\xe4\xb8\x9c\xe7\x9c\x81',
                    '\xe4\xba\x91\xe5\x8d\x97\xe7\x9c\x81',
                    '\xe4\xb8\x8a\xe6\xb5\xb7\xe5\xb8\x82',
                    '\xe6\xb2\xb3\xe5\x8c\x97\xe7\x9c\x81',
                    '\xe5\xb1\xb1\xe4\xb8\x9c\xe7\x9c\x81',
                    '\xe9\x9d\x92\xe6\xb5\xb7\xe7\x9c\x81',
                    '\xe6\xb9\x96\xe5\x8c\x97\xe7\x9c\x81',
                    '\xe6\xb5\x99\xe6\xb1\x9f\xe7\x9c\x81',
                    '\xe6\xb9\x96\xe5\x8d\x97\xe7\x9c\x81',
                    '\xe9\xbb\x91\xe9\xbe\x99\xe6\xb1\x9f\xe7\x9c\x81',
                    '\xe5\xb9\xbf\xe8\xa5\xbf\xe5\xa3\xae\xe6\x97\x8f\xe8\x87\xaa\xe6\xb2\xbb\xe5\x8c\xba',
                    '\xe8\xb4\xb5\xe5\xb7\x9e\xe7\x9c\x81',
                    '\xe5\xae\x89\xe5\xbe\xbd\xe7\x9c\x81',
                    '\xe5\x8c\x97\xe4\xba\xac\xe5\xb8\x82',
                    '\xe5\x90\x89\xe6\x9e\x97\xe7\x9c\x81',
                    '\xe5\xb1\xb1\xe8\xa5\xbf\xe7\x9c\x81',
                    '\xe9\x87\x8d\xe5\xba\x86\xe5\xb8\x82',
                    '\xe5\x86\x85\xe8\x92\x99\xe5\x8f\xa4\xe8\x87\xaa\xe6\xb2\xbb\xe5\x8c\xba',
                    '\xe7\x94\x98\xe8\x82\x83\xe7\x9c\x81',
                    '\xe6\xb1\x9f\xe8\xa5\xbf\xe7\x9c\x81',
                    '\xe5\xa4\xa9\xe6\xb4\xa5\xe5\xb8\x82',
                    '\xe6\xb5\xb7\xe5\x8d\x97\xe7\x9c\x81',
                    '\xe6\x96\xb0\xe7\x96\x86\xe7\xbb\xb4\xe5\x90\xbe\xe5\xb0\x94\xe8\x87\xaa\xe6\xb2\xbb\xe5\x8c\xba',
                    '\xe5\xae\x81\xe5\xa4\x8f\xe5\x9b\x9e\xe6\x97\x8f\xe8\x87\xaa\xe6\xb2\xbb\xe5\x8c\xba',
                    '\xe6\xbe\xb3\xe9\x96\x80\xe7\x89\xb9\xe5\x88\xa5\xe8\xa1\x8c\xe6\x94\xbf\xe5\x8d\x80',
                    '\xe9\xa6\x99\xe6\xb8\xaf\xe7\x89\xb9\xe5\x88\xa5\xe8\xa1\x8c\xe6\x94\xbf\xe5\x8d\x80',
                    '\xe8\xa5\xbf\xe8\x97\x8f\xe8\x87\xaa\xe6\xb2\xbb\xe5\x8c\xba',
                    '\xe5\x8f\xb0\xe6\xb9\xbe\xe7\x9c\x81',
                    '\xe9\xa6\x99\xe6\xb8\xaf\xe7\x89\xb9\xe5\x88\xa5\xe8\xa1\x8c\xe6\x94\xbf\xe5\x8c\xba',
                    '\xe6\xbe\xb3\xe9\x97\xa8\xe7\x89\xb9\xe5\x88\xa5\xe8\xa1\x8c\xe6\x94\xbf\xe5\x8c\xba'
                    }


    for prov in all_province:

        all_data['orderprovince_is_' + prov] = all_data.orderprovinceanf.str.contains(prov)

    label = all_data.label    ## put the label at the last column

    del all_data['orderprovinceanf']

    del all_data['label']

    all_data = pd.concat([all_data, label], axis=1)

    print "Data manipulation: province done"

    return all_data

# print "dummy variable shape", orderprovinceanf_dummie.shape, factor_order_province_anf_dummie.shape

## ************************************************
# construct training set and test set

def data_split_train_test(all_data):
    X_train, X_test, y_train, y_test = train_test_split(all_data.iloc[:,0:-1],
                                                        all_data.iloc[:,-1],
                                                        test_size = 0.2,
                                                        random_state = 0 )

    return X_train, X_test, y_train, y_test

## ************************************************
# daily test use

def import_data_test_daily_userid(loc):

    test_data = pd.read_table(loc)

    userid = test_data.iloc[:, 0]

    test_data = test_data.iloc[:, 1:]

    # delete useless
    del test_data['factor_rule_quickpay_gray_dealid']

    del test_data['factor_imsi_in_sibship_blacklist']

    print "test data dimension: ", test_data.shape

    test_data = calculate_time(test_data)

    test_data = clean_null_variables(test_data)

    test_data = replace_by_zero('factor_poiid_in_highrisk_list', test_data)

    test_data = province_clean_test_daily(test_data)

    return test_data, userid

def import_data_test_daily(loc):

    test_data = pd.read_table(loc)

    # delete useless
    del test_data['factor_rule_quickpay_gray_dealid']

    del test_data['factor_imsi_in_sibship_blacklist']

    print "test data dimension: ", test_data.shape

    test_data = calculate_time(test_data)

    test_data = clean_null_variables(test_data)

    test_data = replace_by_zero('factor_poiid_in_highrisk_list', test_data)

    test_data = province_clean_test_daily(test_data)

    return test_data

def province_clean_test_daily(test_data):

    all_province = {'\xe6\xb2\xb3\xe5\x8d\x97\xe7\x9c\x81',
                    '\xe7\xa6\x8f\xe5\xbb\xba\xe7\x9c\x81',
                    '\xe9\x99\x95\xe8\xa5\xbf\xe7\x9c\x81',
                    '\xe5\x9b\x9b\xe5\xb7\x9d\xe7\x9c\x81',
                    '\xe8\xbe\xbd\xe5\xae\x81\xe7\x9c\x81',
                    '\xe6\xb1\x9f\xe8\x8b\x8f\xe7\x9c\x81',
                    '\xe5\xb9\xbf\xe4\xb8\x9c\xe7\x9c\x81',
                    '\xe4\xba\x91\xe5\x8d\x97\xe7\x9c\x81',
                    '\xe4\xb8\x8a\xe6\xb5\xb7\xe5\xb8\x82',
                    '\xe6\xb2\xb3\xe5\x8c\x97\xe7\x9c\x81',
                    '\xe5\xb1\xb1\xe4\xb8\x9c\xe7\x9c\x81',
                    '\xe9\x9d\x92\xe6\xb5\xb7\xe7\x9c\x81',
                    '\xe6\xb9\x96\xe5\x8c\x97\xe7\x9c\x81',
                    '\xe6\xb5\x99\xe6\xb1\x9f\xe7\x9c\x81',
                    '\xe6\xb9\x96\xe5\x8d\x97\xe7\x9c\x81',
                    '\xe9\xbb\x91\xe9\xbe\x99\xe6\xb1\x9f\xe7\x9c\x81',
                    '\xe5\xb9\xbf\xe8\xa5\xbf\xe5\xa3\xae\xe6\x97\x8f\xe8\x87\xaa\xe6\xb2\xbb\xe5\x8c\xba',
                    '\xe8\xb4\xb5\xe5\xb7\x9e\xe7\x9c\x81',
                    '\xe5\xae\x89\xe5\xbe\xbd\xe7\x9c\x81',
                    '\xe5\x8c\x97\xe4\xba\xac\xe5\xb8\x82',
                    '\xe5\x90\x89\xe6\x9e\x97\xe7\x9c\x81',
                    '\xe5\xb1\xb1\xe8\xa5\xbf\xe7\x9c\x81',
                    '\xe9\x87\x8d\xe5\xba\x86\xe5\xb8\x82',
                    '\xe5\x86\x85\xe8\x92\x99\xe5\x8f\xa4\xe8\x87\xaa\xe6\xb2\xbb\xe5\x8c\xba',
                    '\xe7\x94\x98\xe8\x82\x83\xe7\x9c\x81',
                    '\xe6\xb1\x9f\xe8\xa5\xbf\xe7\x9c\x81',
                    '\xe5\xa4\xa9\xe6\xb4\xa5\xe5\xb8\x82',
                    '\xe6\xb5\xb7\xe5\x8d\x97\xe7\x9c\x81',
                    '\xe6\x96\xb0\xe7\x96\x86\xe7\xbb\xb4\xe5\x90\xbe\xe5\xb0\x94\xe8\x87\xaa\xe6\xb2\xbb\xe5\x8c\xba',
                    '\xe5\xae\x81\xe5\xa4\x8f\xe5\x9b\x9e\xe6\x97\x8f\xe8\x87\xaa\xe6\xb2\xbb\xe5\x8c\xba',
                    '\xe6\xbe\xb3\xe9\x96\x80\xe7\x89\xb9\xe5\x88\xa5\xe8\xa1\x8c\xe6\x94\xbf\xe5\x8d\x80',
                    '\xe9\xa6\x99\xe6\xb8\xaf\xe7\x89\xb9\xe5\x88\xa5\xe8\xa1\x8c\xe6\x94\xbf\xe5\x8d\x80',
                    '\xe8\xa5\xbf\xe8\x97\x8f\xe8\x87\xaa\xe6\xb2\xbb\xe5\x8c\xba',
                    '\xe5\x8f\xb0\xe6\xb9\xbe\xe7\x9c\x81',
                    '\xe9\xa6\x99\xe6\xb8\xaf\xe7\x89\xb9\xe5\x88\xa5\xe8\xa1\x8c\xe6\x94\xbf\xe5\x8c\xba',
                    '\xe6\xbe\xb3\xe9\x97\xa8\xe7\x89\xb9\xe5\x88\xa5\xe8\xa1\x8c\xe6\x94\xbf\xe5\x8c\xba'
                    }

    for prov in all_province:
        test_data['orderprovince_is_' + prov] = test_data.orderprovinceanf.str.contains(prov)

    del test_data['orderprovinceanf']


    print "Data manipulation: province done"

    return test_data

##  - - - - - - - - - - - - - - - - - - - -
# import clean data, train model
# imputation strategy: replace by the most common item

def imputation_data(X_train, X_test, y_train, y_test):

    variable_names = X_train.columns.values

    X_train = Imputer( strategy='most_frequent' ).fit_transform(X_train)

    X_test = Imputer( strategy='most_frequent' ).fit_transform(X_test)

    X_train = DataFrame(X_train, columns = variable_names)

    X_test = DataFrame(X_test, columns = variable_names)

    y_train = y_train

    y_test= y_test

    return X_train, X_test, y_train, y_test, variable_names

def imputation_data_daily_test(X_train):

    variable_names = X_train.columns.values

    print len(variable_names)

    X_train = Imputer( strategy='most_frequent' ).fit_transform(X_train)

    X_train = DataFrame(X_train, columns = variable_names)

    return X_train

def scale(data):

    std_scaler = preprocessing.StandardScaler()

    result = std_scaler.fit_transform(data)

    return result





