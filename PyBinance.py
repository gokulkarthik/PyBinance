
import numpy as np
import pandas as pd
import requests
import json
from tqdm.notebook import tqdm
from datetime import datetime, timedelta

class BinanceKlinesPuller:
    
    def __init__(self, base_coin, quote_coin, start_time, end_time, interval):
        self.base_api = 'https://api.binance.com'
        self.klines_api = self.base_api + "/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit=1000"
        self.base_coin = base_coin
        self.quote_coin = quote_coin
        self.symbol = self.base_coin + self.quote_coin
        self.start_time = start_time
        self.end_time = end_time
        self.interval = interval
        self.interval_pandas = self.interval.upper()
        if self.interval_pandas[-1] == "M":
            self.interval_pandas = self.interval_pandas[:-1] + "T"
        self.split_time_limits()
        
    def split_time_limits(self):
        self.time_range = pd.date_range(start=self.start_time, 
                                   end=self.end_time,
                                   freq=self.interval_pandas)
        self.time_limits = []
        step = 1000
        for i in range(0, len(self.time_range), step):
            start_time = self.time_range[i]
            if i + step <= len(self.time_range):
                end_time = self.time_range[i+step-1]
            else:
                end_time = self.time_range[-1]
            self.time_limits.append([start_time, end_time])
        
        return len(self.time_limits)
    
    def pull_data(self):
        #print("Making {} API requests".format(len(self.time_limits)))
        data = []
        for i, (start_time, end_time) in tqdm(enumerate(self.time_limits, start=1), total=len(self.time_limits), leave=False):
            start_time = np.int64(start_time.timestamp()*1e3)
            end_time = np.int64(end_time.timestamp()*1e3)
            parameters = [self.symbol, self.interval, start_time, end_time]
            url = self.klines_api.format(*parameters)
            response = requests.get(url)
            if response.status_code == 200:
                rows = json.loads(response.text)
                data.extend(rows)
            if i >= 1000:
                time.sleep(60)
        columns = ["Open-Time", "Open", "High", "Low", "Close", "Volume", "Close-Time", 
                   "Quote-Asset-Value", "Number-of-Trades", 
                   "Taker-Buy-Base-Asset-Volume", "Taker-Buy-Quote-Asset-Volume", "Ignore"]
        if len(data) > 0:
            data = pd.DataFrame(data, columns=columns)
        else:
            return -1
        
        data = self.clean_data(data)
         
        return data
    
    def clean_data(self, data):
        data['Open-Time'] = data['Open-Time'] // 1e3 # ms to s
        data['Close-Time'] = data['Close-Time'] // 1e3 # ms to s
        data['Open-Time'] = pd.to_datetime(data['Open-Time'], unit='s')
        data['Close-Time'] = pd.to_datetime(data['Close-Time'], unit='s')
        
        date_str_format = "%Y-%m-%dT%H:%M:%S"
        open_time_range = list(pd.date_range(start=data['Open-Time'].min(), 
                                             end=data['Open-Time'].max(),
                                             freq=self.interval_pandas))
        open_time_range = [datetime.strftime(t, date_str_format) for t in open_time_range]
        
        open_time_available = [datetime.strftime(pd.Timestamp(t), date_str_format) for t in data['Open-Time'].values]
        open_time_not_available = sorted(list(set(open_time_range) - set(open_time_available)))

        
        columns = ["Open-Time", "Open", "High", "Low", "Close", "Volume", "Close-Time", 
                   "Quote-Asset-Value", "Number-of-Trades", 
                   "Taker-Buy-Base-Asset-Volume", "Taker-Buy-Quote-Asset-Volume", "Ignore"]
        new_data = pd.DataFrame(columns=columns)
        new_data.loc[:, 'Open-Time'] = open_time_not_available
        new_data['Open-Time'] = pd.to_datetime(new_data['Open-Time'], format=date_str_format)
        
        def find_close_time(open_time):
            close_time = pd.date_range(start=open_time, periods=2, freq=self.interval)[-1]
            close_time = close_time - timedelta(seconds=1)
            return close_time
        new_data['Close-Time'] = new_data['Open-Time'].apply(find_close_time)

        data = pd.concat([data, new_data])
        data = data.reset_index(drop=True)
        data = data.sort_values(by=['Open-Time'])
        
        return data
