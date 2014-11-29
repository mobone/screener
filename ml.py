import pandas as pd
import sqlite3 as lite
import numpy as np
from sklearn import svm, cross_validation
from sklearn.ensemble import ExtraTreesClassifier


con = lite.connect('screens.db')
cur = con.cursor()

def get_data(screen_date, price_date):
    result = pd.read_sql("Select `Ticker`,`Beta`, `5 Yr Hist. Div. Growth %` from screens where date = '{0}'".format(screen_date), con, index_col=['Ticker'])
    result = result.rank(pct=True)
    
    if screen_date == '2014-08-19':
        screen_date = '08/25/2014'
    elif screen_date == '2014-09-24':
        screen_date = '09/24/2014'
    elif screen_date == '2014-11-07':
        screen_date = '11/07/2014'
    initial_prices = pd.read_sql("select `Close / Last`, Ticker from price_data where date = '{0}' group by ticker".format(screen_date), con, index_col=['Ticker'])
    final_prices = pd.read_sql("select `Close / Last`, Ticker from price_data where date = '{0}' group by ticker".format(price_date), con, index_col=['Ticker'])
    result = result.convert_objects(convert_numeric=True)
    initial_prices = initial_prices.convert_objects(convert_numeric=True)
    final_prices = final_prices.convert_objects(convert_numeric=True)

    
    final_prices['pct change'] = (final_prices['Close / Last']-initial_prices['Close / Last'])/initial_prices['Close / Last']
    
    
    result = result.join(final_prices['pct change'])
    
    result = result.dropna()
    test_data = pd.DataFrame(result['pct change'])
    
    result = result.drop('pct change', 1)
    print result
    return (result, test_data)


def get_bins(results, test_data, bins = None):
    if bins is None:
        bins = np.linspace(test_data['pct change'].min(), test_data['pct change'].max(), 6)
        
    bin_data = test_data.copy()
    for i in range(1,len(bins)):
        
        bin_data[(test_data>bins[i-1]) & (test_data<bins[i])] = str(i-1)
    
    bin_data.columns = ['bin']
    
    test_data = test_data.join(bin_data['bin'])
    
    return (test_data, bins)
    
    
(result, test_data) = get_data('2014-09-24', '10/24/2014')
(test_data, bins) = get_bins(result, test_data)

# fit model
model = svm.SVC()
model.fit(result, test_data['bin']) 

# test model
(result, test_data) = get_data('2014-11-07', '11/25/2014')
(test_data, bins) = get_bins(result, test_data, bins)

print model.score(result, test_data['bin'])
#print cross_validation.cross_val_score(model, result, test_data['bin'], scoring='accuracy')

f = open('results.csv', 'w')
f.write("ticker, input, predicted bin, actual bin, actual pct\n")
test_sum = 0
for row in result.iterrows():
    check = []
    for i in range(len(row[1])):
        check.append(row[1][i])
    f.write('{0},{1},{2},{3},{4}\n'.format( row[0], str(check), \
     model.predict([check])[0], test_data['bin'][row[0]], test_data['pct change'][row[0]]))
