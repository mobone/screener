import sqlite3 as lite
import pandas as pd
import numpy as np
import glob,datetime
from multiprocessing import Process, Queue, Lock, cpu_count
from time import sleep
import sys

con = lite.connect('screens.db')
cur = con.cursor()

lock = Lock()

def worker(metric_list,df, lock, completed_queue):
	pct_change = df['pct change']
	data_list = []
	
	while metric_list.qsize()>0:
		cur_metrics = metric_list.get()
		cur_metrics_list = cur_metrics.split('+')
        
        
		try:
			# check that each metric can be correlated individually 
			# with a positive correlation
			final_df = None
			for met in cur_metrics_list:
				
				cur_df = pd.DataFrame(data=df[met], columns=[met])
				cur_df[met] = cur_df.rank(pct=True)
				
				
				cur_df = cur_df.join(df['pct change'])
				
				
				if cur_df.corr()['pct change'][met]<0:
					cur_df[met] = cur_df[met].rank(pct=True, ascending=False)
				
				
				if final_df is None:
					final_df = pd.DataFrame(data=cur_df[met], columns=[met])
				else:
					final_df = final_df.join(cur_df[met])
			
			final_df["sum {0}".format(cur_metrics)] = final_df.sum(axis=1)		
			final_df = final_df.join(df['pct change'])
			corr = final_df.corr()['pct change']['sum {0}'.format(cur_metrics)]
			
			if cur_metrics == '5 Yr Div. Yield %+Beta':
				print corr
				final_df = final_df.join(df['Last Close'])
				final_df.to_csv('best.csv')
					
			
			completed_queue.put((cur_metrics, str(corr)))
		except:
			pass
	print ("worker done")
	done = True
        
	
def send_to_db(completed_queue, metric_list):
    cur.execute("DELETE FROM correlations WHERE file_date = {0};".format(filename))
    con.commit()


    while metric_list.qsize()>0 or completed_queue.qsize()>0:
        item = completed_queue.get()
        
        
        #print "Sending {0} {1}".format(item[0], item[1])
        
        cur.execute("INSERT INTO correlations values (\"{0}\",\"{1}\",\"{2}\");".format(filename, item[0], item[1]))
        
        #print completed_queue.qsize()
        
    con.commit()    
    

def load_correlations(metric_list, date):
    # get screen for a given date
    result = pd.read_sql('Select * from screens where date = \'{0}\''.format(date), con,  index_col=['Ticker'])
    # get prices for a given date, to be joined
    final_prices = pd.read_sql('select `Adj Close`, Ticker from price_data where date = \'2014-10-31\' group by ticker', con, index_col=['Ticker'])
    
    
    
    # remove useless columns
    things_to_drop = ['Date','S&P 500', 'Company Name']
    for thing in things_to_drop:
        result = result.drop([thing],1)
    
    # join price data
    result = result.join(final_prices)
    
    # find percent change
    result['Last Close'] = result['Last Close'].convert_objects(convert_numeric=True)
    result['pct change'] = (result['Adj Close']-result['Last Close'])/result['Last Close']
    
    # set as data frame and start processing with metric queue
    df = result
    
    completed = Queue()
    
    # start data processors
    for i in range(cpu_count()):
        p = Process(target = worker, args = (metric_list, df, lock, completed,))
        p.start()
    
    
    # start sql sender
    Process(target = send_to_db, args = (completed,metric_list,)).start()
    
    
    while metric_list.qsize()>0:
        print "Remainging: {0}".format(metric_list.qsize())
        sleep(1)


depth = 0
ignore = ['index', 'Ticker', 'Date', 'S&P 500', 'Company Name','Exchange', 'Sector', 'Last Close']
def generate_metric_list(list_of_items, depth, caller):
	depth += 1
	slicer = 1
	for i in list_of_items:
		if i in ignore: continue
		if caller == '':
			string = '{0}'.format(i)
		else:
			string = '{0}+{1}'.format(caller,i)
		final_list.append(string)

		if depth<3:
			generate_metric_list(list_of_items[slicer:], depth, string)
			slicer+=1

def get_metrics_from_db(filename):
    result = pd.read_sql('Select * from screens where date = \'{0}\''.format(filename), con,  index_col=['Ticker'])
    return result.columns


try:
    filename = sys.argv[1]
    initial_metrics = get_metrics_from_db(filename)
    final_list = []
    generate_metric_list(initial_metrics, depth, '')
    
    metric_queue = Queue()
    for i in final_list:
        metric_queue.put(i)
    
    load_correlations(metric_queue, str(filename))
except Exception as e:
    print e

