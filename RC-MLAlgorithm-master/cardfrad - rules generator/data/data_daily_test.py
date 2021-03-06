import talos

sql_test = u'''
SELECT 
        zeus.userid,
-- param
       get_json_object(paramcontext,'$.outmoney') outmoney,
       get_json_object(paramcontext,'$.orderProvinceAnf') orderProvinceAnf,

-- factor
       get_json_object(factorcontext,'$.factor_bankcard_pay_times_1d') factor_bankcard_pay_times_1d,
       get_json_object(factorcontext,'$.factor_bankcard_userid_cnt_3d') factor_bankcard_userid_cnt_3d,
       get_json_object(factorcontext,'$.factor_bankcard_pay_sum_amount_1d') factor_bankcard_pay_sum_amount_1d,
       get_json_object(factorcontext,'$.factor_bankmobile_diff_notifymobile') factor_bankmobile_diff_notifymobile,
       get_json_object(factorcontext,'$.factor_bizaccid_in_highrisk_list') factor_bizaccid_in_highrisk_list,
       get_json_object(factorcontext,'$.factor_currentTime') factor_currentTime,
       get_json_object(factorcontext,'$.factor_dealid_in_highrisk_list') factor_dealid_in_highrisk_list,
       get_json_object(factorcontext,'$.factor_diff_bankmobileprovince_orderprovinceanf') factor_diff_bankmobileprovince_orderprovinceanf,

       get_json_object(factorcontext,'$.factor_is_sms_verified') factor_is_sms_verified,
       get_json_object(factorcontext,'$.factor_is_uuid_same_common_uuid') factor_is_uuid_same_common_uuid,
       get_json_object(factorcontext,'$.factor_is_uuid_and_ip_same_as_bindcard') factor_is_uuid_and_ip_same_as_bindcard, 
       get_json_object(factorcontext,'$.factor_is_userid_equal_common_userid') factor_is_userid_equal_common_userid,      
       get_json_object(factorcontext,'$.factor_is_userid_bankmobile_equal_common_bankmobile') factor_is_userid_bankmobile_equal_common_bankmobile,
       get_json_object(factorcontext,'$.facotr_is_ordercity_is_trust_city') facotr_is_ordercity_is_trust_city,
       get_json_object(factorcontext,'$.factor_imsi_in_sibship_blacklist') factor_imsi_in_sibship_blacklist,

       get_json_object(factorcontext,'$.factor_mobile_diff_notifymobile') factor_mobile_diff_notifymobile,  
       get_json_object(factorcontext,'$.factor_outmoney') factor_outmoney,

       get_json_object(factorcontext,'$.factor_rule_quickpay_gray_dealid') factor_rule_quickpay_gray_dealid,
       get_json_object(factorcontext,'$.factor_regTime') factor_regTime,

       get_json_object(factorcontext,'$.factor_poi_ip_location_mobile_same_provin') factor_poi_ip_location_mobile_same_provin,
       get_json_object(factorcontext,'$.factor_poi_ip_location_mobile_nil') factor_poi_ip_location_mobile_nil,
       get_json_object(factorcontext,'$.factor_poiid_in_highrisk_list') factor_poiid_in_highrisk_list,

       get_json_object(factorcontext,'$.factor_quicksign_time_medis_from_null_0') factor_quicksign_time_medis_from_null_0,
       get_json_object(factorcontext,'$.factor_quickpay_user_diffprovince_consume_poiid_money1d') factor_quickpay_user_diffprovince_consume_poiid_money1d,
       get_json_object(factorcontext,'$.factor_quickpay_user_diffprovince_consume_poiid_cnts1d') factor_quickpay_user_diffprovince_consume_poiid_cnts1d,
       get_json_object(factorcontext,'$.factor_quickpay_is_order_bindmobile_bankmobile_poiid_province_diff') factor_quickpay_is_order_bindmobile_bankmobile_poiid_province_diff,
       get_json_object(factorcontext,'$.factor_quickpay_is_order_bankmobile_deal_province_diff3') factor_quickpay_is_order_bankmobile_deal_province_diff3,
       get_json_object(factorcontext,'$.factor_quickpay_money_by_bankcard_partner_one_day') factor_quickpay_money_by_bankcard_partner_one_day,
       get_json_object(factorcontext,'$.factor_quickpay_bookingname_is_bankname') factor_quickpay_bookingname_is_bankname,
       get_json_object(factorcontext,'$.factor_quickpay_success_money_by_bankcard_one_day') factor_quickpay_success_money_by_bankcard_one_day,
       get_json_object(factorcontext,'$.factor_quickpay_success_count_by_userid_half_month') factor_quickpay_success_count_by_userid_half_month,
       get_json_object(factorcontext,'$.factor_quickpay_bankcard_consume_money1d') factor_quickpay_bankcard_consume_money1d,
       get_json_object(factorcontext,'$.factor_quickpay_user_consume_deal_money1d') factor_quickpay_user_consume_deal_money1d,

       get_json_object(factorcontext,'$.factor_same_bankmobilecity_bindmobilecity') factor_same_bankmobilecity_bindmobilecity,
       get_json_object(factorcontext,'$.factor_same_bankmobileprovince_bindmobileprovince') factor_same_bankmobileprovince_bindmobileprovince,
       get_json_object(factorcontext,'$.factor_same_bankmobilecity_dealcity') factor_same_bankmobilecity_dealcity,


       get_json_object(factorcontext,'$.factor_trust_user_subip_score_v2') factor_trust_user_subip_score_v2,
       get_json_object(factorcontext,'$.factor_trust_user_uuid_score_v2') factor_trust_user_uuid_score_v2,

       get_json_object(factorcontext,'$.factor_userid_partner_pay_success_count_one_day') factor_userid_partner_pay_success_count_one_day,
       get_json_object(factorcontext,'$.factor_userid_login_too_many_uuid_30d') factor_userid_login_too_many_uuid_30d,
       get_json_object(factorcontext,'$.factor_user_uuid_trust_level_v2') factor_user_uuid_trust_level_v2,
       get_json_object(factorcontext,'$.factor_user_uuid_login_success_user_cnts_7d_10') factor_user_uuid_login_success_user_cnts_7d_10,
       get_json_object(factorcontext,'$.factor_uuid_pay_sum_amount_1d') factor_uuid_pay_sum_amount_1d

  FROM log.rc_zeus_request zeus
  JOIN ba_rc.web_rc_risklog rl
  ON zeus.requestid = rl.requestid AND zeus.requestid != ''
  	AND rl.partner = 2 AND rl.dt  = {dt}
    AND rl.action IN (32, 33) AND rl.`_mt_action` = 'RISKLEVEL'
    AND rand() <= 0.05
  WHERE zeus.dt = {dt}
  AND zeus.sceneid IN (186, 187);
'''

# sql = sql.encode('utf-8')

# date = ['20161001', '20161101', '20161201',
#         '20170101', '20170201', '20170301',
#         '20170401', '20170501', '20170601']

date = ['20170515', '20170516', '20170517',
        '20170615', '20170616', '20170617']

que = []

for dt in date:

    pos = talos.Query(sql_test.format(dt = dt),
                      dump='/Users/hongbo/PycharmProjects/DecisionTree/data/test/test_userid_' + dt + '.txt')
    pos.start()

    que.append(pos)

for query_temp in que:

    print query_temp, "start to join !"

    query_temp.join()

    print query_temp, " done !"

print "All done !"