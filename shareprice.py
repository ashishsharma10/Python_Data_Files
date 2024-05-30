import datetime
from dateutil.relativedelta import relativedelta
import yfinance as yf
import pandas as pd
import pymongo

def downloadStockData(ticker, start_date, end_date):
    try:
        data = yf.download(ticker, start=start_date, end=end_date)
        if data.empty:
            raise ValueError(f"No data found for ticker {ticker}")
        data.reset_index(inplace=True)
        return data[['Date', 'Adj Close']]
    except Exception as e:
        print(f"Failed to download data for {ticker}: {e}")
        return None

def saveStockDataToMongoDB(client, database_name, collection_name, stock_data):
    db = client[database_name]
    collection = db[collection_name]
    collection.insert_many(stock_data.to_dict('records'))
    print("Stock data saved to MongoDB successfully.")

def main(excel_file, start_date, end_date, client_uri, database_name, collection_name):
    # Connect to MongoDB
    client = pymongo.MongoClient(client_uri)

    # Read tickers from Excel file
    tickers = pd.read_excel(excel_file)['Symbol']

    # Download and save data for each stock
    for ticker in tickers:
        stock_data = downloadStockData(ticker, start_date, end_date)
        if stock_data is not None:
            saveStockDataToMongoDB(client, database_name, collection_name, stock_data)

# Specify the path to your Excel file
excel_file = r"/content/NSE_Companies_List_New.xlsx"

# Define the date range
start_date = "2020-04-01"
end_date = datetime.datetime.now().strftime("%Y-%m-%d")  # Current date

# MongoDB client details
client_uri = "mongodb+srv://ashishsharma1085:dZrSE1Xoe8q2ibi1@cluster0.e9xnedu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "StockDatabase"
collection_name = "StockPrices"

# Run the main function to download and save data
main(excel_file, start_date, end_date, client_uri, database_name, collection_name)
