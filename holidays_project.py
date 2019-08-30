import psycopg2
from config import config
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import date
import holidays
from scipy.stats import ttest_ind

def holidays_to_csv():
    #NOTE: I could've simply returned a dataframe, but I wanted practice writing data to CSV files.
    #Retrieve japanese holidays
    japan_holidays = holidays.JP(years=range(1990,2020))

    #write a CSV file
    data = codecs.open(r'C:\tmp\holidays.csv','wb',encoding="utf-8-sig")

    #create writer object
    csvwriter = csv.writer(data)
    count = 0

    #Look at the past 30 years
    for date, name in sorted(japan_holidays.items()):
        #Header
        if count == 0:
            csvwriter.writerow(['Date','Weekday'])
            count += 1
        #Row by row
        csvwriter.writerow([date,day_name[date.weekday()]])

    #close data
    data.close()

def get_rates():
    """ query data from the fxrate table """
    #Establish a connection
    conn = None
    #Connect and run query
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        #query from fxrate table
        cur.execute("SELECT date_t,rate FROM fxrate")
        rows = cur.fetchall()
        #construct a dataframe using the queried info
        df = pd.DataFrame(rows, columns = ['date','rate'])
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            #Close connection and return DF
            conn.close()
            return df

def get_holidays():
    """ query data from the holiday table """
    conn = None
    #Connect and run query
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        #Query from holiday table. Exclude weekends and holidays after today.
        cur.execute("SELECT date_t,weekday FROM holidays WHERE date_t<'8/21/2019' AND NOT weekday = 'Sunday' AND NOT weekday = 'Saturday'")
        rows = cur.fetchall()
        #construct a dataframe using the queried info
        df = pd.DataFrame(rows, columns = ['date','weekday'])
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            #Close connection and return DF
            conn.close()
            return df

if __name__ == '__main__':
    #This was all carried out in a Jupyter notebook environment
    #get the holiday data frame
    holiday_df = get_holidays()
    holiday_df.set_index('date', inplace=True, drop=True)

    #get the rates data frame
    rates_df = get_rates()
    rates_df.set_index('date', inplace=True, drop=True)

    #switch to daily return
    rates_df = rates_df.pct_change()
    rates_df.dropna(inplace=True)

    #merge
    holiday_rates=pd.merge(rates_df,holiday_df, how='inner', left_index=True, right_index=True)
    holiday_rates.drop(['weekday'],axis=1,inplace=True)
    holiday_rates=holiday_rates.astype(float)
    holiday_rates.head()

    #one day before
    past_rate_shift = rates_df.shift(1)
    before_holiday_rates = pd.merge(past_rate_shift,holiday_df, how='inner', left_index=True, right_index=True)
    before_holiday_rates.drop(['weekday'],axis=1,inplace=True)
    before_holiday_rates=before_holiday_rates.astype(float)

    #one day after
    future_rate_shift = rates_df.shift(-1)
    after_holiday_rates = pd.merge(future_rate_shift,holiday_df, how='inner', left_index=True, right_index=True)
    after_holiday_rates.drop(['weekday'],axis=1,inplace=True)
    after_holiday_rates=after_holiday_rates.astype(float)

    #get the rolling averages and volatilities
    rates_df['30 MA'] = rates_df['rate'].rolling(window=30).mean()
    rates_df['30 STD'] = rates_df['rate'].rolling(window=30).std()
    #get the exponentially weighted vols and means
    rates_df['EWMA30'] = rates_df['rate'].ewm(span=30).mean()
    rates_df['EWMV30'] = rates_df['rate'].ewm(span=30).std()

    #Visualize the dataframe
    ax = rates_df[['30 MA']].plot(title = 'Graph 1: Exchange Rate Percent Change 30 Day Moving Average and Holiday Exchange Rate Percent Change',figsize=(12,8))
    holiday_rates['rate'].plot(ax=ax, linestyle = '', marker='.')

    #The test we are running is to determine if the mean exchange rate percent change on a holiday is statisically different than the percent change on the day before or after the holiday
    ttest_ind(holiday_rates['rate'], before_holiday_rates['rate'])
    ttest_ind(holiday_rates['rate'], after_holiday_rates['rate'])
