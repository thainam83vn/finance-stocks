from yahoo_finance import Share
from pymongo import MongoClient

client = MongoClient()
db = client['finance']
colStocks = db["stocks"]
colLogs = db['logs']
colTransactions = db['transactions']

stocks = colStocks.find({},{'_id':0,'Symbol': 1})
i = 0
for stock in stocks:
    symbol = stock["Symbol"]
    stock["Share"] = Share(symbol)
    print stock["Share"].get_price()
    i = i + 1
    if i > 10:
        break


#yahoo = Share('YHOO')

#print yahoo.get_price()
