import sqlite3 as lite
import sys
import pandas as pd
con = lite.connect('screens.db')
cur = con.cursor()


try:
    filename = sys.argv[1]
    metrics = sys.argv[2]
    original_metrics = metrics
    metrics = metrics.replace('+', '", "')
    metrics = '"' + metrics +'"'
    result = pd.read_sql('Select Date, Ticker, {0}, "Last Close" from screens where date = \'{1}\''.format(metrics, filename), con,  index_col=['Ticker'])
    final_prices = pd.read_sql('select `Adj Close`, Ticker from price_data where date = \'2014-10-31\' group by ticker', con, index_col=['Ticker'])
    result = result.join(final_prices)
    result['Sum'] = 0
    
    
    metrics = original_metrics.split('+')
    
    for i in metrics:
        result['Sum'] += result[i].convert_objects(convert_numeric=True)
    result['Last Close'] = result['Last Close'].convert_objects(convert_numeric=True)
    result['pct change'] = (result['Adj Close']-result['Last Close'])/result['Last Close']
    result.to_csv('{0}.csv'.format(original_metrics))
except Exception as e:
    print e
    
    
