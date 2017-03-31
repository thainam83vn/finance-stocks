from yahoo_finance import Share
from pymongo import MongoClient

client = MongoClient()
db = client['finance']
stockColl = db["stocks"]
logger = db['logs']
transactionColl = db['transactions']

yahoo = Share('YHOO')

print yahoo.get_price()
