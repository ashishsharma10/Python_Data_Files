import yfinance as yf
import pandas as pd
import pymongo
import json
from datetime import datetime, timezone

def getUserRequiredData(requiredData, companySymbol):
    baseClass = yf.Ticker(f"{companySymbol.upper()}.NS")
    if requiredData == 'info':
        return formatInfo(baseClass)
    elif requiredData == 'actions':
        return formatActions(baseClass)
    elif requiredData == 'shares':
        return formatShares(baseClass)
    elif requiredData == 'incomeStatement':
        return formatIncomeStatement(baseClass)
    elif requiredData == 'balanceSheet':
        return formatBalanceSheet(baseClass)
    elif requiredData == 'cashFlow':
        return formatCashFlow(baseClass)
    elif requiredData == 'recommendation':
        return formatRecommendations(baseClass)
    else:
        return None  # Handle unknown requiredData

def formatInfo(data):
    return data.info

def formatActions(data):
    df = data.actions 
    return addDateToData(df)

def formatIncomeStatement(data):
    df = data.income_stmt
    return addDateToData(df)

def formatBalanceSheet(data):
    df = data.balance_sheet
    return addDateToData(df)

def formatCashFlow(data):
    df = data.cashflow
    return addDateToData(df)

def formatRecommendations(data):
    df = data.recommendations
    return json.loads(df.to_json(orient='records', date_format='iso'))

def formatShares(data):
    da = data.get_shares_full(start="2022-01-01", end=None)
    df = pd.DataFrame(list(da.items()), columns=['Date', 'Value'])
    df['Date'] = df['Date'].astype(str)
    return json.loads(df.to_json(orient='records'))

def addDateToData(tableData):
    tableData.reset_index(inplace=True)
    return json.loads(tableData.to_json(orient='records'))


def convert_epoch_to_mongodb_date(epoch_timestamp):
    # Convert epoch timestamp to datetime object (considering it's in seconds)
    date_obj = datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc)
    
    # Convert datetime object to milliseconds since epoch
    milliseconds_since_epoch = int(date_obj.timestamp() * 1000)
    
    # MongoDB Date type requires the date in milliseconds since epoch
    mongo_date = {"$date": milliseconds_since_epoch}
    
    return mongo_date

def convert_epoch_to_iso8601(epoch_timestamp):
    # Convert epoch timestamp (in milliseconds) to datetime object
    date_obj = datetime.fromtimestamp(epoch_timestamp / 1000.0, tz=timezone.utc)  # Convert to UTC datetime

    # Format datetime object to ISO 8601 string
    iso8601_string = date_obj.isoformat()

    return iso8601_string


excel_file_path = 'NSE_COMPANIES_LIST.xlsx'
df = pd.read_excel(excel_file_path)
# client = pymongo.MongoClient('mongodb+srv://ashishsharma1085:dZrSE1Xoe8q2ibi1@cluster0.e9xnedu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
# db = client['india_stock_data']

# for symbol in df['Symbol']:
#     if pd.notna(symbol) and symbol.strip():
#         fields = ['info', 'actions', 'shares', 'incomeStatement', 'balanceSheet','cashFlow', 'recommendation' ]
#         for field in fields:
#              collection = db[field]
#              document = {'symbol': symbol, 'data': getUserRequiredData(field,symbol)}
#              result = collection.insert_one(document)
#              print(f'{field} for {symbol}')       


newData = {}

document =  getUserRequiredData('balanceSheet','TCS')

for obj in document:
    newObj = {}
    for keys in obj:
        if keys != 'index':
            newObj[convert_epoch_to_iso8601(int(keys))] = obj[keys]
            # print(newObj,'<=================')
    # print(obj['index'])
    # print(newObj, '<------------------------------')
    newData[obj['index']] = newObj

print(newData)