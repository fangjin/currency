#!/usr/bin/env python
import os
import string
import sys
import math
import copy
import scipy
import numpy
from numpy import *
import numpy as np
from scipy import linalg
import sqlite3
import matplotlib.pyplot as plt 
#import interest2db
import calculator
from unidecode import unidecode
from datetime import datetime,timedelta
import argparse
import sqlite3 as lite
import json
import boto
from etool import queue

__version__ = "0.0.1"

def	get_domain(conn,domain_name):
	conn.create_domain(domain_name)
	return conn.get_domain(domain_name)

def	multiRegree(yArray, xArray1, xArray2, xArray3):
	'''this fucntion is used to estimate multiple regression model coefficients'''

	if(len(yArray)!=len(xArray1)):
		print "the length of yArray and xArray1 doesn't match!"
		return

	if(len(yArray)!=len(xArray2)):
		print "the length of yArray and xArray2 doesn't match!"
		return

	if(len(yArray)!=len(xArray3)):
		print "the length of yArray and xArray3 doesn't match!"
		return
	
	k11=0.0
	k12=0.0
	k13=0.0
	k22=0.0
	k23=0.0
	k33=0.0

	c1=0.0
	c2=0.0
	c3=0.0
	
	### x1*x1
	for i in range(0,len(xArray1)):
		k11=k11+xArray1[i]*xArray1[i]

	### x2*x2
	for i in range(0,len(xArray1)):
		k22=k22+xArray2[i]*xArray2[i]

	### x3*x3
	for i in range(0,len(xArray1)):
		k33=k33+xArray3[i]*xArray3[i]

	### x1*x2
	for i in range(0,len(xArray2)):
		k12=k12+xArray1[i]*xArray2[i]
	print k12, " k12"

	### x1*x3
	for i in range(0,len(xArray3)):
		k13=k13+xArray1[i]*xArray3[i]

	### x2*x3
	for i in range(0,len(xArray3)):
		k23=k23+xArray2[i]*xArray3[i]

	### x1*y
	for i in range(0,len(xArray1)):
		c1=c1+xArray1[i]*yArray[i]

	### x2*y
	for i in range(0,len(xArray2)):
		c2=c2+xArray2[i]*yArray[i]

	### x3*y
	for i in range(0,len(xArray3)):
		c3=c3+xArray3[i]*yArray[i]
	A=np.array([[k11, k12, k13],[k12, k22, k23],[k13, k23, k33]])
	B=np.array([c1, c2, c3])
	print A	
	print B	
	Coeff=linalg.solve(A,B)
	
	#Coeff=np.linalg.lstsq(A,B)[0]	
	return Coeff

def check_if_tradingday(conn,predictiveDate,currency_index,country):
    "Check if the day weekend"
    weekDay = datetime.strptime(predictiveDate,"%Y-%m-%d").weekday()
    if weekDay == 5 or weekDay == 6:
        log.info("%s For %s is Weekend, Just Skip!" %(predictiveDate,currency_index))
        return False
    
    
    "Check if the day is holiday"
    t_domain = conn.get_domain('s_holiday')
    sql = "select count(*) from s_holiday where country = '{}'".format(country)
    rs = t_domain.select(sql)
    count = 0
    for r in rs:
        count = int(r['Count'])
    if count == 0:
        return True
    else:
        log.info( "%s For %s is Holiday, Just Skip!" %(predictiveDate,stockIndex))
        return False

def	getZscore(conn,cur_date,currency_index,cur_diff,duration):
	t_domain = get_domain(conn,"t_enriched_bloomberg_prices")
	scores = []
	sql = "select oneDayChange from t_enriched_bloomberg_prices where post_date<'{}' and name = '{}' order by post_date desc".format(cur_date,currency_index)
	rows = t_domain.select(sql,max_items=duration)
	for row in rows:
		scores.append(row["oneDayChange"])
	zscore = calculator.calZscore(scores, cur_diff)
	return zscore


def	parse_args():
	ap = argparse.ArgumentParser("Process the currency data")
	default_day = datetime.strftime(datetime.now() + timedelta(days =1),"%Y-%m-%d")	
	ap.add_argument('-p',dest="predict_day",metavar="PREDICT DAY", type=str,default=default_day,nargs="?",help="The day to be predicted: %y-%m-%d")
	ap.add_argument('-conf',dest="conf_f",metavar="CONFIG",type=str,nargs="?",default="./Regression_Fitting.conf",help='The path of config')
	ap.add_argument('-c',dest="currency_list",metavar="CURRENCY LIST",type=str,nargs="+",help='The list of currency')
	ap.add_argument('-kd',dest="key_id",metavar="KeyId for AWS",type=str,help="The key id for aws")
	ap.add_argument('-sr',dest="secret",metavar="secret key for AWS",type=str,help="The secret key for aws")
	ap.add_argument('-zmq',dest="zmq_port",metavar="ZMQ",type=str,help="ZMQ Host and Port,format( tcp://host:port )")
	return ap.parse_args()

def	get_traing_data(conn,predict_date,fitting_num,group_num,currency):
	t_domain = get_domain(conn,"t_enriched_bloomberg_prices")
	sql = "select oneDayChange,postDate,embersId from t_enriched_bloomberg_prices where name = '{}' and postDate < '{}' order by postDate desc".format(currency,predict_date)
	rs = t_domain.select(sql,max_items=fitting_num)
	delta_currency = []
	derived_from = []
	group_interest = []
	group_inflation = []
	group_invest = []

	for r in rs:
		delta_currency.append(100*r["oneDayChange"])
		post_date = r["postDate"]
		derived_from.append(r["embersId"])

		"Get training interest,inflation,invest"
		end_date = datetime.strptime(post_date,"%Y-%m-%d") + timedelta(days = -1)
		start_date = datetime.strptime(post_date,"%Y-%m-%d") + timedelta(days = -(group_num+1))
		sql = "select embersId,interest,inflation,invest from t_currency_surrogate where currency_index='{}' and post_date >= '{}' and post_date <= '{}'".format(currency,start_date,end_date)
		q_domain = get_domain(conn,"t_currency_surrogate")
		iii_results = q_domain.select(sql)
		sum_interest = 0
		sum_inflation = 0
		sum_invest = 0
		for iii_r in iii_results:
			derived_from.append(iii_r["embersId"])
			sum_interest += iii_r["interest"]
			sum_inflation += iii_r["inflation"]
			sum_invest += iii_r["invest"]
			
		
		group_interest.append(sum_interest)
		group_inflation.append(sum_inflation)
		group_invest.append(sum_invest)

	return delta_currency,group_interest,group_inflation,group_invest,derived_from

def	get_predict_data(conn,predict_date,group_num,currency):
	p_domain = get_domain(conn,"t_currency_surrogate")
	derived_from = []
	"Get training interest,inflation,invest"
	end_date = datetime.strptime(predict_date,"%Y-%m-%d") + timedelta(days = -1)
	start_date = datetime.strptime(predict_date,"%Y-%m-%d") + timedelta(days = -(group_num+1))
	sql = "select embersId,interest,inflation,invest from t_currency_surrogate where currency_index='{}' and post_date >= '{}' and post_date <= '{}'".format(currency,start_date,end_date)
	iii_results = p_domain.select(sql)
	sum_interest = 0
	sum_inflation = 0
	sum_invest = 0
	for iii_r in iii_results:
		derived_from.append(iii_r["embersId"])
		sum_interest += iii_r["interest"]
		sum_inflation += iii_r["inflation"]
		sum_invest += iii_r["invest"]
	return sum_interest,sum_inflation,sum_invest
	

def	predict(conn,currency,predict_date,CONFIG):
	"Check trading day"
	country = CONFIG["location"][currency]
	flag = check_if_tradingday(conn,predictiveDate,currency,country)
	if flag is not True:
		return None
	
	"Get training parameters"
	fitting_num = CONFIG["fitting_num"][currency]
	group_num = CONFIG["group_num"][currency]

	"Get the trainging data"
	training_delta_currency,training_group_interest,training_group_inflation,training_group_invest,derived_from = get_traing_data(conn,predict_date,fitting_num,group_num,currency)
	Y = np.array(training_delta_currency)
	A1 = np.array(training_group_interest)
	A2 = np.array(training_group_inflation)
	A3 = np.array(training_group_invest)
	
	"Get the predict data"
	predict_interest,predict_inflation,predict_invest = get_predict_data(conn,predict_date,group_num,currency)
	
	"Fitting the model"	
	Amatrix = np.array([A1,A2,A3]).T	
	estimateCoeff=np.linalg.lstsq(Amatrix,Y)[0]
	#estimateCoeff=multiRegree(training_delta_currency,training_group_interest,training_group_inflation,training_group_invest)
	
	
	"forecast the delta currency"
	estimated_delta_currency = (predict_interest*estimateCoeff[0]+predict_inflation*estimateCoeff[1]+predict_invest*estimateCoeff[2])/100

	"Check if trigger the warning"
	estimated_zscore30 = 0
	estimated_zscore30 = 0
	#Y_delta_estimated.append(estimated_delta_currency)
	
	"compute Z-score" # select one_day_change from t_enriched_bloomberg_prices where 
	
	estimated_zscore30 = getZscore(conn,predict_date,currency,estimated_delta_currency,30)
	estimated_zscore90 = getZscore(conn,predict_date,currency,estimated_delta_currency,90)
	event_type = "0000"
	z30_bottom = CONFIG["warning_threshold"]["zscore30"][0]
	z30_up = CONFIG["warning_threshold"]["zscore30"][1]		
	z90_bottom = CONFIG["warning_threshold"]["zscore90"][0]
	z90_up = CONFIG["warning_threshold"]["zscore90"][1]		

	if estimated_zscore30 >= z30_up or estimated_zscore90 >= z90_up:
		event_type = "0421"
	elif estimated_zscore30 <= z30_bottom or estimated_zscore90 <= z90_bottom:
		event_type = "0422"

	"construct warning message"
	warningMessage = {}
	warningMessage["derivedFrom"] = {"derivedIds":derived_from}
        warningMessage["model"] = "Delta Regression Model"
        warningMessage["eventType"] = event_type
        warningMessage["confidence"] = 0.80
        warningMessage["confidenceIsProbability"] = True
        warningMessage["eventDate"] = predict_date
        warningMessage["population"] = currency
        warningMessage["location"] = CONFIG["location"][currency]
        warningMessage["version"] = __version__
        operateTime = datetime.now().isoformat()
        warningMessage["dateProduced"] = operateTime
        config_version = {"configVersion":CONFIG["version"]}
        warningMessage["comments"] = config_version
        warningMessage["description"] = "Use Delta Regression model to predict currency sigma events"

	print currency,"______",predict_date,"____" ,estimated_delta_currency, "____",event_type
	print warningMessage
	return warningMessage
	

def	main():
	args = parse_args()

	predict_date = args.predict_day
	conf_f = args.conf_f
	cur_list = args.currency_list
	key_id = args.key_id
	secret = args.secret
	zmq_port = args.zmq_port
	
	conn = boto.connect_sdb(key_id,secret)

	all_config = json.load(open(conf_f))
	"Get the latest version of CONFIG "	
	latest_version = max([int(k) for k in all_config.keys()])
	CONFIG = all_config[str(latest_version)]
	if cur_list is None:
		cur_list = CONFIG["currency_list"]
	
	with queue.open(zmq_port, 'w', capture=False) as outq:
		for currency in cur_list:
			prediction = predict(conn,currency,predict_date,CONFIG)
			if prediction and prediction["eventType"]!="0000":
				"push message to ZMQ"
				outq.write(prediction)


if __name__ == "__main__":
	    main()

	



