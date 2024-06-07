import datetime
from dateutil.relativedelta import relativedelta
import yfinance as yf
import pandas as pd
import pymongo
import json

def downloadIndexData(index_symbol, start_date, end_date):
    try:
        data = yf.download(index_symbol, start=start_date, end=end_date)
        if data.empty:
            raise ValueError(f"No data found for index {index_symbol}")
        data.reset_index(inplace=True)
        return data[['Date', 'Adj Close']]
    except Exception as e:
        print(f"Failed to download data for index {index_symbol}: {e}")
        return None

def saveIndexDataToMongoDB(client, database_name, collection_name, index_data):
    db = client[database_name]
    collection = db[collection_name]
    collection.insert_many(index_data.to_dict('records'))
    print("Index data saved to MongoDB successfully.")

def main(excel_file, index_column_name, start_date, end_date, client_uri, database_name, collection_name):
    # Connect to MongoDB
    client = pymongo.MongoClient(client_uri)

    # Read index symbols from Excel file
    try:
        index_symbols = pd.read_excel(excel_file)[index_column_name]
    except KeyError:
        print(f"Column '{index_column_name}' not found in the Excel file.")
        return

    # Download and save data for each index
    for index_symbol in index_symbols:
        if pd.notna(index_symbol):  # Check if the value is not NaN
            index_data = downloadIndexData(index_symbol, start_date, end_date)
            if index_data is not None:
                saveIndexDataToMongoDB(client, database_name, collection_name, index_data)

# Specify the path to your Excel file containing index symbols
excel_file = r"/content/Index Data.xlsx"

# Specify the column name containing index symbols in your Excel file
index_column_name = "Symbol"

# Define the date range
start_date = "2020-04-01"
end_date = datetime.datetime.now().strftime("%Y-%m-%d")  # Current date

# MongoDB client details
client_uri = "mongodb+srv://ashishsharma1085:dZrSE1Xoe8q2ibi1@cluster0.e9xnedu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "IndexDatabase"
collection_name = "IndexPrices"

# Run the main function to download and save index data
main(excel_file, index_column_name, start_date, end_date, client_uri, database_name, collection_name)
