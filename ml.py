import pandas as pd
import sqlite3 as lite
import numpy as np
from sklearn import svm

con = lite.connect('screens.db')
cur = con.cursor()

result = pd.read_sql("Select `Ticker`,`Beta`, `PEG Ratio`, `Book Value`, `5 Yr Hist. Div. Growth %`, `Quick Ratio`, `Last Close` from screens where date = '2014-09-24'", con, index_col=['Ticker'])

final_prices = pd.read_sql('select `Adj Close`, Ticker from price_data where date = \'2014-11-21\' group by ticker', con, index_col=['Ticker'])
result['Last Close'] = result['Last Close'].convert_objects(convert_numeric=True)

final_prices['pct change'] = (final_prices['Adj Close']-result['Last Close'])/result['Last Close']


result = result.drop(['Last Close'], 1)

result = result.dropna()

test_data = result[100:].join(final_prices[100:])



clf = svm.SVC()
clf.fit(result[:100], final_prices['pct change'][:100])  

f = open('results.csv', 'w')
f.write("ticker, input, predicted, actual, difference\n")
test_sum = 0
for row in result[200:].iterrows():
    check = []
    print row[1]
    for i in range(len(row[1])):

        check.append(row[1][i])


    f.write('{0},{1},{2},{3},{4}\n'.format( row[0], str(check).replace(",",""), \
     clf.predict([check])[0], final_prices['pct change'][row[0]], clf.predict([check])[0] - final_prices['pct change'][row[0]]))
    
    test_sum = abs( clf.predict([check])[0] - final_prices['pct change'][row[0]] )
    
print test_sum







