import datetime
from dateutil.relativedelta import relativedelta
import yfinance as yf
import pandas as pd
import pymongo
import json

def downloadIndexData(index_symbol, dates):
    try:
        # Create an empty DataFrame to store the results
        all_data = pd.DataFrame()

        # Loop through each date and download data for that date
        for date in dates:
            start_date = date - datetime.timedelta(days=2)
            end_date = date + datetime.timedelta(days=2)
            data = yf.download(index_symbol, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
            if data.empty:
                raise ValueError(f"No data found for index {index_symbol} on date {date}")
            data.reset_index(inplace=True)
            # Filter the data for the exact date
            data_filtered = data[data['Date'] == date.strftime("%Y-%m-%d")].copy()
            if not data_filtered.empty:
                data_filtered['Ticker'] = index_symbol  # Add the ticker column
                all_data = pd.concat([all_data, data_filtered[['Date', 'Adj Close', 'Ticker']]])

        return all_data
    except Exception as e:
        print(f"Failed to download data for index {index_symbol}: {e}")
        return None

def saveIndexDataToMongoDB(client, database_name, collection_name, index_data):
    db = client[database_name]
    collection = db[collection_name]
    collection.insert_many(index_data.to_dict('records'))
    print("Index data saved to MongoDB successfully.")

def main(excel_file, index_column_name, dates, client_uri, database_name, collection_name):
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
            index_data = downloadIndexData(index_symbol, dates)
            if index_data is not None:
                saveIndexDataToMongoDB(client, database_name, collection_name, index_data)

# Specify the path to your Excel file containing index symbols
excel_file = r"C:\Users\Ashish\Desktop\MBA\Project Work\python test\Python_Data_Files\NSE_Companies_List_New.xlsx"

# Specify the column name containing index symbols in your Excel file
index_column_name = "Symbol"

# Generate the list of March 31st dates for the last 5 years
end_date = datetime.datetime.now()
dates = [datetime.datetime(end_date.year - i, 3, 31) for i in range(1, 6)]

# MongoDB client details
client_uri = "mongodb+srv://ashishsharma1085:dZrSE1Xoe8q2ibi1@cluster0.e9xnedu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "StockPrices5y"
collection_name = "StockPrice5y"

# Run the main function to download and save index data
main(excel_file, index_column_name, dates, client_uri, database_name, collection_name)
