#!/usr/bin/env python3

from yfinance import Ticker
from pprint import pprint
from utils import save_data
import pandas as pd
from datetime import datetime

tickers = ["AAPL", "MSFT", "AMZN", 
           "META", "NTFX", "GOOG"]

def collect_stock_data(stock_ticker: str):
    """
    
    """
    ticker_data =  Ticker(stock_ticker)
    start_date =  datetime.strptime("2020-01-01", "%Y-%m-%d").strftime("%Y-%m-%d")
    end_date =  datetime.strptime("2020-02-01", "%Y-%m-%d").strftime("%Y-%m-%d")
    try:
        df = ticker_data.history(interval="1mo", start=start_date, end=end_date)
        df['date'] = datetime.now().strftime("%Y-%m-%d")
        if len(df) == 1:
            return df
    except Exception as e:
        print("Error collecting data for ticker {}: {}".format(stock_ticker, e))

for ticker in tickers:
    stock_data = collect_stock_data(ticker)
    current_date = datetime.now().strftime("%Y%m%d")
    file_name = f"{current_date}_stock_{ticker}.csv"
    if stock_data is not None:
        save_data(stock_data, file_name, file_type="csv", 
              zone="raw", context="stocks")