import pandas as pd
from datetime import date

#Import Bid/ask files, convert to London time zone.

#Bid
gj_10_12_B = pd.read_csv('C:/Users/ralph/Downloads/GBPJPY_Candlestick_1_M_BID_01.01.2010-31.12.2012.csv')
gj_13_15_B = pd.read_csv('C:/Users/ralph/Downloads/GBPJPY_Candlestick_1_M_BID_01.01.2013-31.12.2015.csv')
gj_16_18_B = pd.read_csv('C:/Users/ralph/Downloads/GBPJPY_Candlestick_1_M_BID_01.01.2016-31.12.2018.csv')
gj_19_21_B = pd.read_csv('C:/Users/ralph/Downloads/GBPJPY_Candlestick_1_M_BID_01.01.2019-14.12.2021.csv')

gj_1m_b = gj_10_12_B.append(gj_13_15_B).append(gj_16_18_B).append(gj_19_21_B)

gj_1m_b['Local time'] = gj_1m_b['Local time'].str.slice(0,19)
gj_1m_b['Local time'] = pd.to_datetime(gj_1m_b['Local time'], format = '%d.%m.%Y %H:%M:%S')
gj_1m_b.index = gj_1m_b['Local time']
gj_1m_b = gj_1m_b.tz_localize('GMT')
gj_1m_b = gj_1m_b.tz_convert('Europe/London')
gj_1m_b = gj_1m_b.tz_localize(None)    

gj_1m_b = gj_1m_b[['Open', 'High', 'Low', 'Close']]

#Ask
gj_10_12_A = pd.read_csv('C:/Users/ralph/Downloads/GBPJPY_Candlestick_1_M_ASK_01.01.2010-31.12.2012.csv')
gj_13_15_A = pd.read_csv('C:/Users/ralph/Downloads/GBPJPY_Candlestick_1_M_ASK_01.01.2013-31.12.2015.csv')
gj_16_18_A = pd.read_csv('C:/Users/ralph/Downloads/GBPJPY_Candlestick_1_M_ASK_01.01.2016-31.12.2018.csv')
gj_19_21_A = pd.read_csv('C:/Users/ralph/Downloads/GBPJPY_Candlestick_1_M_ASK_01.01.2019-14.12.2021.csv')

gj_1m_a = gj_10_12_A.append(gj_13_15_A).append(gj_16_18_A).append(gj_19_21_A)

gj_1m_a['Local time'] = gj_1m_a['Local time'].str.slice(0,19)
gj_1m_a['Local time'] = pd.to_datetime(gj_1m_a['Local time'], format = '%d.%m.%Y %H:%M:%S')
gj_1m_a.index = gj_1m_a['Local time']
gj_1m_a = gj_1m_a.tz_localize('GMT')
gj_1m_a = gj_1m_a.tz_convert('Europe/London')
gj_1m_a = gj_1m_a.tz_localize(None)    

gj_1m_a = gj_1m_a[['Open', 'High', 'Low', 'Close']]

#Join and calc mid price
gj_1m = gj_1m_b.join(gj_1m_a, lsuffix='_bid', rsuffix='_ask')

gj_1m['Open'] = (gj_1m['Open_bid'] + gj_1m['Open_ask'])/2
gj_1m['High'] = (gj_1m['High_bid'] + gj_1m['High_ask'])/2
gj_1m['Low'] = (gj_1m['Low_bid'] + gj_1m['Low_ask'])/2
gj_1m['Close'] = (gj_1m['Close_bid'] + gj_1m['Close_ask'])/2

gj_1m = gj_1m[['Open', 'High', 'Low','Close']]

gj_1m = gj_1m.rename_axis("datetime")

#Bulk Upload

years = gj_1m.index.year.unique()
months = range(1,13)
current_year = date.today().year
current_month = date.today().month

for year in years:
    for month in months:
        if (current_year > year) | (current_month >= month):
            data_filter = gj_1m[(gj_1m.index.year == year) & (gj_1m.index.month == month)]
            file_loc = 's3://bucket-fxdata/gbpjpy/oneminute/'+str(year)+'_'+str(month).zfill(2)+'.csv'
            data_filter.to_csv(file_loc)


gj_5m = gj_1m.resample('5T').agg({'Open': 'first', 
                                  'High': 'max', 
                                  'Low': 'min', 
                                  'Close': 'last'})

for year in years:
    for month in months:
        if (current_year > year) | (current_month >= month):
            data_filter = gj_5m[(gj_5m.index.year == year) & (gj_5m.index.month == month)]
            file_loc = 's3://bucket-fxdata/gbpjpy/fiveminute/'+str(year)+'_'+str(month).zfill(2)+'.csv'
            data_filter.to_csv(file_loc)