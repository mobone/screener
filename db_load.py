import sqlite3 as lite
import pandas as pd
import numpy as np
import glob,datetime
from multiprocessing import Process, Queue, Lock, cpu_count
from time import sleep
import sys

con = lite.connect('screens.db')
cur = con.cursor()

def load_to_db(filename):
    # get correct tickers and columns to use
    result = pd.read_sql('Select * from screens where date = \'2014-11-07\'', con)
    
    csv_dataframe = pd.read_csv(filename)	
    # remove the unnamed column
    for i in csv_dataframe.columns:
        if i not in result.columns: csv_dataframe = csv_dataframe.drop(i, 1)
    
    # remove symbos that sql does not like
    formatted_metrics = []
    for i in csv_dataframe.columns:
        formatted_metrics.append(i.replace('[','(').replace(']',')'))
    csv_dataframe.columns = formatted_metrics

    # replace NaNs with None, or null as sql sees it
    csv_dataframe = csv_dataframe.where(pd.notnull(csv_dataframe), None)

    # set data column to the filename
    csv_dataframe['Date'] = filename.replace('.csv', '').replace('./screens/','')

	# only store tickers that are in the sp500
    csv_dataframe = csv_dataframe[csv_dataframe['Ticker'].isin(result['Ticker'])]
    
    # send to db
    csv_dataframe.to_sql('screens', con, if_exists='append')

def get_prices(ticker):
    con = lite.connect('screens.db')
    cur = con.cursor()
    now = datetime.datetime.now()
    start_month = 7
    start_day = 1
    start_year = 2014
    end_month = now.month - 1
    end_day = now.day
    end_year = now.year

    try:
        url ='http://ichart.yahoo.com/table.csv?s={6}&a={0}&b={1}&c={2}&d={3}&e={4}&f={5}&g=d&ignore=.csv'.format(
                    start_month, start_day, start_year, end_month, end_day, end_year, ticker)
        price_data = pd.read_csv(url)

        price_data['Ticker'] = np.array(['{0}'.format(ticker)]*len(price_data))
        price_data = price_data[['Date','Ticker','Adj Close']]
        
        price_data.to_sql('price_data', con, if_exists='append')
        
    except Exception as e:
        print e

def load_prices_to_db():
    result = cur.execute('SELECT ticker from screens group by ticker')
    tickers = result.fetchall()
    tick_count = 0
    for ticker in tickers:
        get_prices(ticker[0])
        tick_count+=1
        print tick_count/float(len(tickers))     


try:
	filename = sys.argv[1]
	filename = "./screens/" + filename + ".csv"
	load_to_db(filename)
except Exception as e:
	print e
	

load_prices_to_db()
