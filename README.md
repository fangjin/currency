currency
========This model is try to collect previous 5 days' news, to extract interest rate, inflation and invest to do linear regressioin,
to predict the next day's delta currency. 

Data preprocessing function is in "articleEnricher.py", it will read one news file every night, each file news including
many articles. The program will read each article and push to "t_currency_enriched" table, and also sum everyday's article 
and push to "t_currency_surrogate" table.

Model prediction is doing in "Regresion_Fitting.py". Here we will do prediction, select the previous 5 days factors from 
t_currency_surrogate, and call regression. After getting the delta currency, we will calculte zscore30 and zscore90, if then
event_type is none zero, we will push to ZMQ.