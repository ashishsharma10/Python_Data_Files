import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import yfinance as yf
import pandas as pd
from scipy import stats
import pymongo

def downloadData(ticker):
    start, end = datetime.datetime.now() - relativedelta(years=5), datetime.datetime.now()
    try:
        data = yf.download(ticker, start=start, end=end)
        if data.empty:
            raise ValueError(f"No data found for ticker {ticker}")
        data.reset_index(inplace=True)
        data.set_index("Date", inplace=True)
        return data
    except Exception as e:
        print(f"Failed to download data for {ticker}: {e}")
        return None

def processBeta(stockData, marketData, year, frequency, adjustment=0):
    stockData, marketData = shortenData(stockData, marketData, year)
    stockData = stockData['Adj Close'].resample(frequency).last().pct_change().dropna()
    marketData = marketData['Adj Close'].resample(frequency).last().pct_change().dropna()
    
    # Ensure we have matching dates for both stock and market data
    stockData = stockData[stockData.index.isin(marketData.index)]
    marketData = marketData[marketData.index.isin(stockData.index)]
    
    if len(stockData) < 2 or len(marketData) < 2:
        print(f"Not enough data to calculate beta for this period.")
        return None, None
    
    beta = calculateBeta(stockData, marketData)
    regressionData = stats.linregress(marketData, stockData)
    return adjustBeta(beta, adjustment), regressionData

def shortenData(stockData, marketData, year):
    if year == 5:
        return stockData, marketData
    array1 = stockData.index > datetime.datetime.now() - relativedelta(years=year)
    array2 = marketData.index > datetime.datetime.now() - relativedelta(years=year)
    stockData = stockData[array1]
    marketData = marketData[array2]
    return stockData, marketData

def calculateBeta(stockData, marketData):
    covariance = np.cov(stockData, marketData)
    variance = np.var(marketData)
    return covariance[0,1] / variance

def adjustBeta(beta, adjustment):
    for i in range(adjustment):
        beta = 0.67 * beta + 0.33
    return beta

def beta(excel_file, market='^BSESN', adjusted=0):
    # Connect to MongoDB
    client = pymongo.MongoClient('mongodb+srv://ashishsharma1085:dZrSE1Xoe8q2ibi1@cluster0.e9xnedu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
    db = client["BetaDatabase"]
    collection = db["BetaValues"]

    market = downloadData(market)
    if market is None:
        print(f"Failed to download market data for {market}")
        return

    stocks = pd.read_excel(excel_file)['Symbol']
    for ticker in stocks:
        stock = downloadData(ticker)
        if stock is None:
            continue
        
        beta1m5y, _ = processBeta(stock, market, 5, '1M', adjusted)
        beta1m3y, _ = processBeta(stock, market, 3, '1M', adjusted)
        beta1w5y, _ = processBeta(stock, market, 5, '1W', adjusted)
        beta1w3y, _ = processBeta(stock, market, 3, '1W', adjusted)
        beta1w1y, _ = processBeta(stock, market, 1, '1W', adjusted)
        beta1d1y, _ = processBeta(stock, market, 1, '1D', adjusted)
        
        # Check if beta values were calculated successfully
        if any(b is None for b in [beta1m5y, beta1m3y, beta1w5y, beta1w3y, beta1w1y, beta1d1y]):
            print(f"Not enough data to calculate beta for {ticker}")
            continue

        # Round beta values to 2 decimal places
        beta1m5y = round(beta1m5y, 2)
        beta1m3y = round(beta1m3y, 2)
        beta1w5y = round(beta1w5y, 2)
        beta1w3y = round(beta1w3y, 2)
        beta1w1y = round(beta1w1y, 2)
        beta1d1y = round(beta1d1y, 2)

        print(f'''{ticker} Betas:
Monthly 5 Years {beta1m5y}
Monthly 3 Years {beta1m3y}
Weekly  5 Years {beta1w5y}
Weekly  3 Years {beta1w3y}
Weekly  1 Year  {beta1w1y}
Daily   1 Year  {beta1d1y}''')

        # Store the results in MongoDB
        beta_data = {
            "ticker": ticker,
            "betas": {
                "Monthly 5 Years": beta1m5y,
                "Monthly 3 Years": beta1m3y,
                "Weekly 5 Years": beta1w5y,
                "Weekly 3 Years": beta1w3y,
                "Weekly 1 Year": beta1w1y,
                "Daily 1 Year": beta1d1y
            }
        }
        collection.insert_one(beta_data)

# Specify the path to your Excel file
excel_file = r"/content/NSE_Companies_List_New.xlsx"

# Run the beta function
beta(excel_file)
