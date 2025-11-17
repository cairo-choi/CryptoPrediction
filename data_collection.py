# Data collection libraries
import yfinance as yf
import requests
import os
import pandas as pd
import datetime as dt


# Parent Class
class BaseCollection():
  def __init__(self, name, start_date = '2017-01-01', end_date = '2025-06-15'):
    self.name = name
    self.df = None
    self.start_date = start_date
    self.end_date = end_date

  
  def save(self, save_path, data): 
    try: 
        self.df = data
        if self.df is None:
            raise ValueError("Dataframe is empty")
        # if path not exist, then generate it 
        os.makedirs(save_path, exist_ok=True)
        save_path = f"{save_path}/{self.name}.parquet"  
        self.df.to_parquet(save_path, index=True)  #save as parquet format

        print(f'{self.name} has been sucessfully saved')

        return self.df  # return pandas dataframe
    
    except Exception as e: 
        print(f'Error occurred while saving data: {e}')
        
        return None
        

  def normalization(self, data):
    # 1. normalize as pandas df
    if not isinstance(data, pd.DataFrame):
        try:
            df = pd.DataFrame(data)
        except Exception as e:
            print(f"Your data cannot be converted to pandas df: {e}")
            return None
    else:
        df = data.copy()
    
    
    if isinstance(df.columns, pd.MultiIndex) and 'Ticker' in df.columns.names:
        df = df.droplevel('Ticker', axis=1)
    # 2. normalize time zone as utc
    try:
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        else:
            df.index = df.index.tz_convert('UTC')
    except Exception as e:
        print(f"Error converting timezone: {e}")
        return None
    df.index = df.index.normalize()
    # print(df)

    df = df.reset_index().rename(columns = {'index':'Date'})
   
    df['Date'] = df['Date'].dt.date
    
    # print(df)
    self.start_date = pd.to_datetime(self.start_date).date()
    # print(self.start_date)
    # print(type(self.start_date))
    self.end_date = pd.to_datetime(self.end_date).date()
    # print(self.end_date)
    df = df[(df['Date'] >= self.start_date) & (df['Date'] <= self.end_date)]
    self.df = df
    
    return self.df


# Collecting asset OHLCV data via yfinance API
class AssetCollection(BaseCollection):
  def __init__(self, name, start_date, end_date):
    super().__init__(name, start_date, end_date) 

  def fetch_data(self, ticker, interval = '1d'):
      df = None
      try:
          data = yf.download(ticker, start = self.start_date, end=self.end_date)
          
          if data.empty:
                print(f"No data returned for {ticker}")

          # check if datetime is utc or not
          df = self.normalization(data)  

          self.df = df

          return self.df
       
      except Exception as e:
          print(f"Error fetching data for {ticker}: {e}")
          self.df = None
          return None    
      


# Collecting hash rate data via Blockchain API
class HashrateCollection(BaseCollection):
  def __init__(self, name, start_date, end_date):
    super().__init__(name, start_date, end_date) 


  def fetch_data(self, sampled = False):
      url = "https://api.blockchain.info/charts/hash-rate"
      params = {
            'timespan': 'all',  # Since the Blockchain.com API does not support start and end parameters, the fetched data have to be filtered on the client side to select the desired date range.
            'format': 'json',
            'sampled': str(sampled).lower()
            }
      try:
          response = requests.get(url, params=params)
          response.raise_for_status() # if response is not 200, raise exception
          data = response.json() # collected as json fomat
          df = pd.DataFrame(data=data['values'])  # create dataframe from 'values' in json data 

          df = df.rename(columns={'x': 'Date', 'y': 'Hashrate'})   # x -> date, y -> hashRate
          df['Date'] = pd.to_datetime(df['Date'],unit='s', utc=True)
          df.set_index('Date', inplace=True) # set Date as index
          # normalization
          df = self.normalization(df)

          # filter by start_date and end_date
        #   start_date = pd.to_datetime(start_date, utc=True)
        #   end_date = pd.to_datetime(end_date, utc=True)
          

          self.df = df
          return self.df
      
      except Exception as e:
          print(f"Error fetching data: {e}")
          self.data = None
          return None


# running main 
def main():
    # # define save path
    save_path = './data'
    start_date = '2017-01-01'
    end_date = '2025-06-16'
    # # print(f'{save_path}')

    # set btc, eth, xp500, xrp, ltc tickers
    # btc_ticker = 'BTC-USD'
    # eth_ticker = 'ETH-USD'
    # sp500_ticker = '^GSPC'
    # xrp_ticker = 'XRP-USD'
    # ltc_ticker = 'LTC-USD'

    # tickers = [btc_ticker, eth_ticker, sp500_ticker, xrp_ticker, ltc_ticker]

    # for ticker in tickers:
    #    asset = AssetCollection(ticker, start_date, end_date)
    #    df = asset.fetch_data(ticker)
    # #    print(df)
    #    asset.save(save_path, df)
       



    # collect 

    hashrate = HashrateCollection('hashrate', start_date, end_date)
    hash_data = hashrate.fetch_data()
    print(hash_data)
    #save hash rate data into defined path 
    hashrate.save(save_path, hash_data)

if __name__ == "__main__":
   main()
    
   
   