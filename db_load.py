import sqlite3 as lite
import pandas as pd
import numpy as np
import glob,datetime
from multiprocessing import Process, Queue, Lock, cpu_count
from time import sleep
import time
import sys
import urllib2, urllib
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

def get_prices(ticker_queue):
    con = lite.connect('screens.db')
    cur = con.cursor()
    user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
    values = {'name' : 'Michael Foord', 'location' : 'Northampton', 'language' : 'Python' }
    headers = { 'User-Agent' : user_agent }
    while ticker_queue.qsize()>0:
        ticker = ticker_queue.get()
        while True:
            try:                
                ticker = ticker.replace('.', '-')
                print ticker
                data = urllib.urlencode(values)
                req = urllib2.Request("http://www.nasdaq.com/symbol/"+ticker+"/historical", data, headers)
                response = urllib2.urlopen(req)
                
                data = response.read()
                results = pd.read_html(data)
                
                results = pd.DataFrame(results[3])
                results['Ticker'] = ticker
                results = results.drop('Open', 1)
                results = results.drop('High', 1)
                results = results.drop('Low', 1)
                results = results.drop('Volume', 1)
                results.to_sql('price_data', con, if_exists='append')
                print ticker_queue.qsize()/500.0
                break
            except Exception as e:
                print e
                sleep(1)
            
    """
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
        price_data = pd.read_html(url)

        price_data['Ticker'] = np.array(['{0}'.format(ticker)]*len(price_data))
        price_data = price_data[['Date','Ticker','Adj Close']]
        
        price_data.to_sql('price_data', con, if_exists='append')
        
    except Exception as e:
        print e
    """
def load_prices_to_db():
    date='2999-10-10'
    try:
        result = cur.execute('select Date from price_data order by date desc limit 1')
        date = result.fetchone()[0]
    except:
        print 'Database query failed'
    now = time.strftime("%Y-%m-%d")
    
    if date>now or True:
        print 'Updating Prices'
        try:
            result = cur.execute('DELETE from price_data')
            con.commit()
        except Exception as e:
            print e
        result = cur.execute('SELECT ticker from screens group by ticker')
        tickers = result.fetchall()
        tick_count = 0
        ticker_queue = Queue()
        for ticker in tickers:
            ticker_queue.put(str(ticker[0]))
        for i in range(cpu_count()*2):
            p = Process(target = get_prices, args = (ticker_queue,))
            p.start()
        while ticker_queue.qsize()>0:
            sleep(1)
        
            

# loading screen file into db
if len(sys.argv) > 1:
	filename = sys.argv[1]
	filename = "./screens/" + filename + ".csv"
	load_to_db(filename)


# method checks if there are new 
# prices and if there are, delete 
# the old prices and get new ones
load_prices_to_db()
