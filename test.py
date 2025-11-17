# Data collection libraries
import yfinance as yf
import requests
import os
import pandas as pd
import datetime as dt

start_date = '2017-01-01'
end_date = '2025-06-16'
data = yf.download('BTC-USD', start=start_date, end=end_date)
print(data)