##Import all libraries needed
from datetime import date, datetime, timedelta
import requests
import time
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.impute import SimpleImputer
import yfinance as yf

#Initalizing the importat Constants
START_DAY = date(year=2024, month=7, day=31)
SYMBOL = "DOGEUSD"
NUM_DAYS = 31

COLUMN_LIST = ['open', 'high', 'low', 'close', 'volume']

POLYGON_API_KEY = "d3tIobwoExakvIhj9u0XisNdsSt7n57O"
POLYGON_REST_BASEURL = "https://api.polygon.io"
SYMBOL_PREFIX = "X:"

# Month of data distributed by day using polygon

def build_request_url(symbol, start_time, end_time, multiplier, timespan, limit, sort):
    symbol_with_prefix = SYMBOL_PREFIX + symbol

    return (
        f"{POLYGON_REST_BASEURL}/v2/aggs/ticker/{symbol_with_prefix}"
        f"/range/{multiplier}/{timespan}/{start_time}/{end_time}"
        f"?sort={sort}&limit={limit}&apiKey={POLYGON_API_KEY}"
    )

def get_timestamp_range(date):
    start_time = datetime.combine(date, datetime.min.time())
    end_time = start_time + timedelta(days=1)
    start_timestamp = int(start_time.timestamp() * 1000)
    end_timestamp = int(end_time.timestamp() * 1000) - 1

    return start_timestamp, end_timestamp

def fetch_1m_data(symbol, date):
    start_timestamp, end_timestamp = get_timestamp_range(date)
    request_url = build_request_url(symbol, start_timestamp, end_timestamp, 1, "minute", 50000, "asc")

    response = requests.get(request_url)
    data = response.json()

    if "results" not in data:
        raise Exception(f"Did not receive appropriate data for {symbol} on {date}")

    df = pd.DataFrame(data["results"])
    df["time"] = pd.to_datetime(df["t"], unit="ms")
    df = df[["time", "o", "h", "l", "c", "v"]]
    df = df.rename(columns={"time":"date",
                            "o": "open",
                            "h": "high",
                            "l": "low",
                            "c": "close",
                            "v": "volume"})
    df.set_index('date',inplace = True)
    return df

def collect_historical_data(symbol, start_date, days):
    all_data = []
    current_date = start_date

    for _ in range(days):
        print(f"Fetching data for {current_date}")
        try:
            daily_data = fetch_1m_data(symbol, current_date)
            all_data.append(daily_data)
        except Exception as e:
            print(f"Error fetching data for {current_date}: {e}")
        current_date -= timedelta(days=1)
        time.sleep(15)

    return pd.concat(all_data)

# 5 years data distributed by day using yfinance

def create_daily_df(ticker):
  daily_ticker = yf.Ticker(ticker)
  df = daily_ticker.history(start = '2019-07-01',end = '2024-08-01',interval = '1d')
  df = df.drop(['Dividends','Stock Splits'],axis = 1)
  df.columns = df.columns.str.lower()
  df.index.names = ['date']
  df.index = df.index.tz_localize(None)
  return df

#The function fills the missing values by getting the mean values for the hour

def fill_missing_values(df):
    df.index = pd.to_datetime(df.index)

    df_resampled = df.resample('T').asfreq()

    hourly_means = df.resample('H').mean()

    for hour, hour_group in df_resampled.groupby(df_resampled.index.floor('H')):
        df_resampled.loc[hour_group.index, COLUMN_LIST] = \
            hour_group[COLUMN_LIST].fillna(hourly_means.loc[hour])

    return df_resampled

# Backfilling the data

def generate_minute_data(df_daily, models):

    all_minutes = pd.date_range(df_daily.index.min(), df_daily.index.max(), freq='T')
    df_all_minutes = pd.DataFrame(index=all_minutes)

    for date in df_daily.index:
        daily_data = df_daily.loc[date]
        day_minutes = df_all_minutes[df_all_minutes.index.normalize() == date]
        num_minutes = len(day_minutes)

        if num_minutes == 0:
            continue

        for column in COLUMN_LIST[:4]:
            column_series = np.random.normal(
                loc=daily_data[column],
                scale=0.05 * daily_data[column],
                size=num_minutes
            )
            df_all_minutes.loc[day_minutes.index, column] = column_series

        volume_series = np.random.normal(
            loc=daily_data['volume'] / num_minutes,
            scale=0.5 * daily_data['volume'] / num_minutes,
            size=num_minutes
        )
        df_all_minutes.loc[day_minutes.index, 'volume'] = np.maximum(volume_series, 0)

    return df_all_minutes

# Function to compare the accuracy of the model

def plot_column_comparison(df_daily, df_all_minutes, column):

    df_all_minutes.index = pd.to_datetime(df_all_minutes.index)
    df_daily.index = pd.to_datetime(df_daily.index)

    if column not in df_all_minutes.columns or column not in df_daily.columns:
        raise ValueError(f"Column '{column}' is not valid. Ensure it exists in both DataFrames.")

    df_daily_from_minutes = df_all_minutes.resample('D').agg({
        column: 'first'
    })

    plt.figure(figsize=(10, 6))
    plt.plot(df_daily.index, df_daily[column], label=f'Daily {column}', color='blue', marker='o')
    plt.plot(df_daily_from_minutes.index, df_daily_from_minutes[column], label=f'Backfilled {column}', color='red', linestyle='--')
    plt.title(f'{column.capitalize()} Comparison')
    plt.xlabel('Date')
    plt.ylabel('Value')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

def main():
  df_minute = collect_historical_data(SYMBOL, START_DAY, NUM_DAYS)
  df_minute = fill_missing_values(df_minute)

  df_daily = create_daily_df("DOGE-USD")

  df_all_minutes = generate_minute_data(df_daily, df_minute)
  print(df_all_minutes)

  for column in COLUMN_LIST[:4]:
    plot_column_comparison(df_daily, df_all_minutes, column)

if __name__ == '__main__':
  main()

