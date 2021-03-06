import pandas as pd
import datetime
from datetime import date, timedelta
import yfinance as yf

def lambda_handler(event, context): 
    #Date Setup
    current_year = date.today().year
    current_month = date.today().month
    current_day = date.today().day

    extract_start_day = str(max(current_day - 6,1)).zfill(2)
    extract_start = str(current_year)+"-"+str(current_month)+"-"+extract_start_day
    extract_end = date.today() + timedelta(days=10)

    #Current Data
    current_file_loc = 's3://bucket-fxdata/gbpjpy/oneminute/'+str(current_year)+'_'+str(current_month).zfill(2)+'.csv'
    try:
        current_data = pd.read_csv(current_file_loc)
        current_data.columns = ['datetime','Open','High','Low','Close']
        current_data['datetime'] = pd.to_datetime(current_data['datetime'] , format = '%Y-%m-%d %H:%M:%S')
        current_data = current_data.set_index('datetime')
    except:
        old_file_loc = 's3://bucket-fxdata/gbpjpy/oneminute/2021_12.csv'
        old_data = pd.read_csv(old_file_loc)
        old_data.columns = ['datetime','Open','High','Low','Close']
        old_data['datetime'] = pd.to_datetime(old_data['datetime'] , format = '%Y-%m-%d %H:%M:%S')
        old_data = old_data.set_index('datetime')
        current_data = old_data.head(0)

    #New Data
    new_data = yf.download(tickers = 'GBPJPY=X' , start=extract_start, end=extract_end, interval = '1m')
    new_data = new_data.tz_localize(None)   
    new_data = new_data[['Open', 'High', 'Low','Close']]

    #Combined Data
    full_data = current_data.append(new_data)
    unique_data = full_data[~full_data.index.duplicated(keep='last')]

    #Upload
    unique_data.to_csv(current_file_loc)
    
    #5 Minute resample
    unique_data_5m = unique_data.resample('5T').agg({'Open': 'first', 
                                  'High': 'max', 
                                  'Low': 'min', 
                                  'Close': 'last'})
                                  
    current_file_loc_5m = 's3://bucket-fxdata/gbpjpy/fiveminute/'+str(current_year)+'_'+str(current_month).zfill(2)+'.csv'
    
    print(unique_data_5m)
    #unique_data_5m.to_csv(current_file_loc_5m)

    return "Done"