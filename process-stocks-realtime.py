import csvapi
import web3
from pymongo import MongoClient
import time
import datetime

URL_OPTION = 'saboc1p2pghwva5b6k3a2ed1t1'
COLUMNs = ['Symbol', 'Ask', 'Bid', 'Open', 'Change','ChangePercent','PreviousClose','DayLow','DayHigh','52WeekRange',
           'Volume','AskSize','BidSize','LastTradeSize','AvgDailyVolume',
           'Earning','TradeDate', 'TradeTime']
URL_UPDATE_MARKET = 'http://download.finance.yahoo.com/d/quotes.csv?bypass=true&s={stocks}&f=' + URL_OPTION
client = MongoClient()
db = client['finance']
stockColl = db["stocks"]
logger = db['logs']
transactionColl = db['transactions']

cursor = stockColl.find({}, {'Market':1, 'Symbol': 1})
stocks = []
tradeTimes = {}
for doc in cursor:
    stocks = stocks + [doc]

def updateMarket():
    i = 0
    while i < len(stocks):
        noStocks = 400
        if i + noStocks > len(stocks) - 1:
            noStocks = len(stocks) - 1 - i
        arr = stocks[i:noStocks]
        stockNames = ','.join(d["Symbol"] for d in arr)
        url = URL_UPDATE_MARKET.format(stocks = stockNames)
        print(datetime.datetime.now(), ": call from ", i, " to ", i + noStocks)
        data = web3.download(url)
        print(datetime.datetime.now(), ": updating to db")
        list = csvapi.csvToList(COLUMNs, data)
        for item in list:
            if item['Symbol'] in tradeTimes:
                if tradeTimes[item['Symbol']] != item['TradeTime']:
                    transactionColl.insert(item)
                    tradeTimes[item['Symbol']] = item['TradeTime']
            else:
                transactionColl.insert(item)
                tradeTimes[item['Symbol']] = item['TradeTime']

        i = i + 400
        time.sleep(10)

while True:
    now = datetime.datetime.now().hour - 4
    if 1:
        print(datetime.datetime.now(), ": updating market")
        logger.insert({'DateTime': datetime.datetime.now(), 'Description': 'Begin Update transactions.'})
        updateMarket()
        logger.insert({'DateTime': datetime.datetime.now(), 'Description': 'End Update transactions.'})
        print(datetime.datetime.now(), ": done updating market")
    print('Waiting for next 10 minutes')
    time.sleep(60*10)

