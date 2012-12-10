#!/usr/bin/env python
from datetime import date, timedelta, datetime
import os
import string
import sys
import json
import phraseSearch
from datetime import datetime 
from unidecode import unidecode
#extract previous 5 days news from bloomberg files, according to the 7 countries respectively

factor_dic = { "interest": [[('interest-rate', 'value') , 0], [('interest', 'value') , 0], [('interests', 'value') , 0], [('interest-rates', 'value') , 0], [('real', 'value'),('interest', 'value'), 1],[('interest', 'value'),('rate', 'value'), 1] ,[('interest', 'value'),('rates', 'value'), 1]],  "inflation": [[('inflation', 'value') , 0], [('cpi', 'value') , 0], [('consumer-price', 'value') , 0],[('consumer', 'value'),('price', 'value'), 1], [('consumer', 'value'),('prices', 'value'), 1], [('price', 'value'),('index', 'value'), 1]] ,  "invest": [[('invest','value'), 0], [('invested','value'), 0], [('investors','value'), 0], [('investing','value'), 0], [('investments','value'), 0], [('investment','value'), 0], [('benefit','value'), 0], [('profit','value'), 0], [('boot','value'), 0], [('boost','value'), 0], [('boosted','value'), 0], [('boosting','value'), 0], [('foreign', 'value'),('invest', 'value'), 1], [('foreign', 'value'),('investment', 'value'), 1] ,[('net', 'value'),('flow', 'value'), 1], [('foreign', 'value'),('investors', 'value'), 1]] }


sentiment = json.load(open("/home/jf/Currency/integrate-code/Basis_enriched_version/sentiment_dic.json"))

def isodate(date):
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



def extract_document(file_name ,factorJson):
    #global phraseList, phraseConf
    indicator=open("/home/jf/Currency/integrate-code/Basis_enriched_version/bloomberg_news_indicator.conf","r")
    indicator1=json.load(indicator)    
   
    j = 0
    for curIndex in currency_index:
    with open(file_name) as f:
        for line in f:
            j += 1
            #print j
            article = unidecode(line)
            try:
               articleJson = json.loads(unidecode(article))
               artDate = isodate(articleJson['date'])               
            except ValueError:
               logger.debug("unable to load json line number %s" % j)
               continue
            tokens = articleJson['BasisEnrichment']['tokens']
            reqCurrencyWords = set(indicator1[currency_index])
            if any([True for k in tokens if k['value'] in reqCurrencyWords]):
                sOffsets = [k[0] for k in enumerate(tokens) if k[1]['POS'] == 'SENT']
                sOffsets.insert(0, 0)
                sOffsets.append(len(tokens) - 1)
		sentences = [[tokens[sOffsets[i]:sOffsets[i + 1]]] for i in range(0, len(sOffsets) - 1)]
		articleJson['currencyWord'] = {}
                factorJson[articleJson['embersId']]={}
		for w in factor_dic:                    
                    factorJson[articleJson['embersId']][w] = 0
                    sentArt = 0
                    #s = ' '.join([k['value'] for k in sentence[0]])
                    #print s.encode('utf-8')
                    for sentence in sentences:
                        phraseSearch.getPhrase(factor_dic[w])
                        #print phraseSearch.phraseList
                        result = phraseSearch.filterByPhrase(sentence)
                        #print result
                        if result[0]:
                            if w in articleJson['currencyWord']:
                                articleJson['currencyWord'][w].append(sentence)
                            else:
                                articleJson['currencyWord'][w] = [sentence[0]]
                        negWords = [1 for k in sentence[0] if k['value'] in sentiment['negative_word']]
                        posWords = [1 for k in sentence[0] if k['value'] in sentiment['positive_word']]
                        sentArt += sum(posWords) - sum(negWords)
                        #print sentArt
                        #factorJson['embersId'] = {w: sentArt}
                    factorJson[articleJson['embersId']][w] = sentArt
    print "factorJson" % factorJson
    return factorJson
		
                              
                            #@print result 
                #print articleJson['currencyWord']
                                   

#extract_document("/home/jf/Currency/integrate-code/Basis_enriched_version/_file/bloomberg-news-content-basis-2012-11-13-19-08-01","2012-11-11","USDARS")


#fivedays_all
# ["baba"] # refer to onecountry
