# -*- coding: utf-8 -*-
import sqlite3 as lite
import json
import sqlite3
import sys
from unidecode import unidecode
from datetime import datetime
import phraseSearch
import hashlib
from datetime import datetime
import argparse
import os
import boto
from etool import queue

factor_dic = { "interest": [[('interest-rate', 'value') , 0], [('interest', 'value') , 0], [('interests', 'value') , 0], [('interest-rates', 'value') , 0], [('real', 'value'),('interest', 'value'), 1],[('interest', 'value'),('rate', 'value'), 1] ,[('interest', 'value'),('rates', 'value'), 1]],  "inflation": [[('inflation', 'value') , 0], [('cpi', 'value') , 0], [('consumer-price', 'value') , 0],[('consumer', 'value'),('price', 'value'), 1], [('consumer', 'value'),('prices', 'value'), 1], [('price', 'value'),('index', 'value'), 1]] ,  "invest": [[('invest','value'), 0], [('invested','value'), 0], [('investors','value'), 0], [('investing','value'), 0], [('investments','value'), 0], [('investment','value'), 0], [('benefit','value'), 0], [('profit','value'), 0], [('boot','value'), 0], [('boost','value'), 0], [('boosted','value'), 0], [('boosting','value'), 0], [('foreign', 'value'),('invest', 'value'), 1], [('foreign', 'value'),('investment', 'value'), 1] ,[('net', 'value'),('flow', 'value'), 1], [('foreign', 'value'),('investors', 'value'), 1]] }

def	isodate(date):
	try:
		return datetime.strptime(date.strip(), '%Y-%m-%dT%H:%M:%S.%f')
	except ValueError:
		try:
			return datetime.strptime(date.strip(), '%Y-%m-%dT%H:%M:%S')
		except ValueError:
			try:
				return datetime.strptime(date.strip(), '%Y-%m-%d')
			except ValueError:
				try:
					return datetime.strptime(date.strip(), '%Y-%m')
				except ValueError:
	      				try:
		  				return datetime.strptime(date.strip(), '%Y')
	      				except ValueError:
		  				log.error('date could not be decoded: %s' % date)
	return None

def 	get_domain(conn,domain_name):
	conn.create_domain(domain_name)
	return conn.get_domain(domain_name)

def	store_message(domain,message):
	domain.put_attributes(message["embersId"],message)

def	insert_enricheddata(conn,enriched_data):
	t_domain = get_domain(conn,"t_currency_enriched")
	store_message(t_domain,enriched_data)

def 	enrich(conn,conf_file,file_name):

	CONFIG = json.load(open(conf_file))
	indicator = CONFIG["indicator"]
	sentiment = CONFIG["sentiment"]

	j = 0
	factorJson = {}
	with open(file_name) as f:
		for index in indicator:
			factorJson[index] = {}
		for line in f:
			j += 1
			article = unidecode(line)
			try:
				articleJson = json.loads(unidecode(article))
				artDate = isodate(articleJson['postDate']).strftime('%Y-%m-%d')               
			except ValueError:
				logger.debug("unable to load json line number %s" % j)
				continue
			tokens = articleJson['BasisEnrichment']['tokens']
			for index in indicator:
				reqCurrencyWords = set(indicator[index])
				sentences = []
				if any([True for k in tokens if k['value'] in reqCurrencyWords]):
					sOffsets = [k[0] for k in enumerate(tokens) if k[1]['POS'] == 'SENT']
					sOffsets.insert(0, 0)
					sOffsets.append(len(tokens) - 1)
					sentences = [[tokens[sOffsets[i]:sOffsets[i + 1]]] for i in range(0, len(sOffsets) - 1)]

				"construct enriched data of each article given currency index"
				enriched_article = {}
				enriched_article["derivedFrom"] = {"derivedIds":[articleJson['embersId']]}
				enriched_article["currencyIndex"] = index
				enriched_article["postDate"] = artDate
				enriched_embersId = hashlib.sha1(json.dumps(enriched_article)).hexdigest()
				enriched_article["embersId"] = enriched_embersId

				if artDate in factorJson[index]:
					factorJson[index][artDate][enriched_embersId] = {}
				else:
					factorJson[index][artDate] = {enriched_embersId: {}}
				for w in factor_dic:                    
					factorJson[index][artDate][enriched_embersId][w] = 0
					sentArt = 0
					#s = ' '.join([k['value'] for k in sentence[0]])
					#print s.encode('utf-8')
					for sentence in sentences:
						phraseSearch.getPhrase(factor_dic[w])
						#print phraseSearch.phraseList
						result = phraseSearch.filterByPhrase(sentence)
						#print result  #([('is', 'value'), 0], 19) or (False, None)
						if result[0]:
							negWords = [1 for k in sentence[0] if k['value'] in sentiment['negative_word']]
							posWords = [1 for k in sentence[0] if k['value'] in sentiment['positive_word']]
							sentArt += sum(posWords) - sum(negWords)

					factorJson[index][artDate][enriched_embersId][w] = sentArt

				enriched_article["interest"] = factorJson[index][artDate][enriched_embersId]["interest"]
				enriched_article["inflation"] = factorJson[index][artDate][enriched_embersId]["inflation"]
				enriched_article["invest"] = factorJson[index][artDate][enriched_embersId]["invest"]
				insert_enricheddata(conn,enriched_article)   
				with queue.open(ENRICHED_ZMQ, 'w', capture=False) as outq:
                			outq.write(enriched_article)

	"construct surrogata data of each currency by the max day of the files"
	daily_sentiment = {}
	for index in factorJson:
		daily_sentiment[index] = {}
		derivedFrom = []
		sentiment = {"interest":0,"inflation":0,"invest":0}
		"summary the sentiment of articles"
		max_day = max(factorJson[index].keys())
		for day in factorJson[index]:
			for k,v in factorJson[index][day].items():
				derivedFrom.append(k)
				for w in v:
					sentiment[w] += v[w]
		daily_sentiment[index]["postDate"] = max_day
		daily_sentiment[index]["interest"] = sentiment["interest"]
		daily_sentiment[index]["inflation"] = sentiment["inflation"]
		daily_sentiment[index]["invest"] = sentiment["invest"]
		daily_sentiment[index]["derivedFrom"] = {"derivedIds":derivedFrom}
		daily_sentiment[index]["currencyIndex"] = index
		embers_id = hashlib.sha1(json.dumps(daily_sentiment[index])).hexdigest()
		daily_sentiment[index]["embersId"] = embers_id

		insert_dailysentiment(conn,daily_sentiment[index])
		with queue.open(SURROGATE_ZMQ, 'w', capture=False) as outq:
                			outq.write(daily_sentiment[index])
		""		
	return daily_sentiment

def	insert_dailysentiment(conn,daily_sentiment):
	t_domain = get_domain(conn,"t_currency_surrogate")
	store_message(t_domain,daily_sentiment)

def	parse_args():
	ap = argparse.ArgumentParser("Process the currency data")
	ap.add_argument('-c',dest="conf_file",metavar="PROCESS CONFIG",default="./bloomberg_news_indicator.conf",type=str,nargs='?',help='the config file')
	ap.add_argument('-f',dest="news_file",metavar="NEWS FILE", type=str,help="The daily news file")
	ap.add_argument('-kd',dest="key_id",metavar="KeyId for AWS",type=str,help="The key id for aws")
	ap.add_argument('-sr',dest="secret",metavar="secret key for AWS",type=str,help="The secret key for aws")
	ap.add_argument('-fd',dest="file_dir",metavar="FILE DIR",type=str,help="The dir of files to be processed")
	ap.add_argument('-ezmq',dest="enrich_zmq_port",metavar="Enriched data ZMQ",type=str,help="Enriched ZMQ Host and Port")
	ap.add_argument('-szmq',dest="surrogate_zmq_port",metavar="Surrogate ZMQ",type=str,help="Surrogate ZMQ Host and Port")
	return ap.parse_args()
	

def main():
	GLOBAL ENRICHED_ZMQ, SURROGATE_ZMQ
	args = parse_args()
	conf_file = args.conf_file
	news_file = args.news_file
	key_id = args.key_id
	secret = args.secret
	file_dir = args.file_dir
	ENRICHED_ZMQ = args.enrich_zmq_port
	SURROGATE_ZMQ = args.surrogate_zmq_port

	conn = boto.connect_sdb(key_id,secret)
	
	if file_dir is not None:
		files = os.listdir(file_dir)
		for f in files:
			f_name = file_dir + "/" + f	
			enrich(conn,conf_file,f_name)
			print f_name, " Done"	
		pass	
	else:
		enrich(conn,conf_file,news_file)

	if conn:
		conn.close()

if __name__ == "__main__":
	main()


# within one article {'2012-11-12': {u'USDCOP': {u'227081b24a967e696f5c5e72f87203d1e20944ec': {'invest': 0, 'inflation': 0, 'interest': -1}}, u'USDARS': {u'227081b24a967e696f5c5e72f87203d1e20944ec': {'invest': 0, 'inflation': 0, 'interest': -1}}, u'USDCRC': {u'227081b24a967e696f5c5e72f87203d1e20944ec': {'invest': 0, 'inflation': 0, 'interest': -1}}, u'USDMXN': {u'227081b24a967e696f5c5e72f87203d1e20944ec': {'invest': 0, 'inflation': 0, 'interest': 0}}, u'USDBRL': {u'227081b24a967e696f5c5e72f87203d1e20944ec': {'invest': 0, 'inflation': 0, 'interest': 0}}, u'USDPEN': {u'227081b24a967e696f5c5e72f87203d1e20944ec': {'invest': 0, 'inflation': 0, 'interest': 0}}, u'USDCLP': {u'227081b24a967e696f5c5e72f87203d1e20944ec': {'invest': 0, 'inflation': 0, 'interest': 0}}}}


#2   {'2012-11-12': {u'USDCOP': {u'2d363d7aaa636566d98b38dc153300886ca389c4': {'invest': 0, 'inflation': 0, 'interest': 0}}, u'USDARS': {u'2d363d7aaa636566d98b38dc153300886ca389c4': {'invest': 0, 'inflation': 0, 'interest': 0}}, u'USDCRC': {u'2d363d7aaa636566d98b38dc153300886ca389c4': {'invest': 0, 'inflation': 0, 'interest': 0}}, u'USDMXN': {u'2d363d7aaa636566d98b38dc153300886ca389c4': {'invest': 0, 'inflation': 0, 'interest': 0}}, u'USDBRL': {u'2d363d7aaa636566d98b38dc153300886ca389c4': {'invest': 0, 'inflation': 0, 'interest': 0}}, u'USDPEN': {u'2d363d7aaa636566d98b38dc153300886ca389c4': {'invest': 0, 'inflation': 0, 'interest': 0}}, u'USDCLP': {u'2d363d7aaa636566d98b38dc153300886ca389c4': {'invest': 0, 'inflation': 0, 'interest': 0}}}} 

