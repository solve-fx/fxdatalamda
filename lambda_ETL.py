import pandas as pd
import numpy as np
from datetime import date, timedelta

def price_resampler(data, timeframe, timeframe_label):

    resampled_data = data.resample(timeframe).agg({'Open': 'first', 
                                                   'High': 'max', 
                                                   'Low': 'min', 
                                                   'Close': 'last'}).dropna()  

    resampled_data_closed = resampled_data[:-1]

    resampled_data_closed['timeframe'] = timeframe_label

    return resampled_data_closed


def lambda_handler(event, context): 
    current_year = date.today().year
    current_month = date.today().month
    current_day = date.today().day

    extract_start_day = str(max(current_day - 6,1)).zfill(2)
    extract_start = str(current_year)+"-"+str(current_month)+"-"+extract_start_day
    extract_end = date.today() + timedelta(days=10)

    years = [current_year - 1, current_year]
    months = range(1,13)

    filenames = []
    for year in years:
        for month in months:
            if (year < current_year) | (month <= current_month):
                filenames.append(str(year)+"_"+str(month).zfill(2)+".csv")

    filenames = filenames[-6:]


    for i in range(len(filenames)):
        file_loc = 's3://bucket-fxdata/gbpjpy/fiveminute/'+filenames[i]
        df = pd.read_csv(file_loc)
        if i == 0:
            data_5m = df.copy()
        else:
            data_5m = data_5m.append(df)

    data_5m['datetime'] = pd.to_datetime(data_5m.datetime, format = '%Y/%m/%d %H:%M:%S')
    data_5m.index = data_5m['datetime']
    data_5m = data_5m.drop(columns=['datetime'])
    data_5m['timeframe'] = '5m'

    data_15m = price_resampler(data_5m, '15T', '15m')
    data_30m = price_resampler(data_5m, '30T', '30m')
    data_1h = price_resampler(data_5m, '1H', '1h')
    data_5m_offset = data_5m.copy()
    data_5m_offset.index = data_5m_offset.index + pd.DateOffset(hours=2)
    data_4h = price_resampler(data_5m_offset, '4H', '4h')
    data_4h.index = data_4h.index - pd.DateOffset(hours=2)

    data = data_5m.append(data_15m).append(data_30m).append(data_1h).append(data_4h)

    data['hour'] = data.index.hour
    data['hour'] = data['hour'].astype(str).str.zfill(2)

    data['minute'] = data.index.minute
    data['minute'] = data['minute'].astype(str).str.zfill(2)

    data['time'] = data['hour']+" "+data['minute']

    data['candle_size_h'] = abs(data['Open'] - data['High'])
    data['candle_size_l'] = abs(data['Open'] - data['Low'])
    data['candle_size'] = data[['candle_size_h', 'candle_size_l']].max(axis=1) * 100

    data['days_since'] = (pd.to_datetime(date.today()) - data.index).days
    data['days_since'] = data['days_since'] - data['days_since'].min()

    data['bullish'] = np.where(data['Close'] > data['Open'], 1, 0)

    data_last100 = data[data['days_since'] <= 100]

    data_last100['date'] = data_last100.index

    data_last100 = data_last100[['date','timeframe','hour','minute','time','candle_size','bullish','days_since']]
    data_last100.to_csv('s3://bucket-fxdata/gbpjpy_last100.csv', index=False)

    return "done!"