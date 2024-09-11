from libraries import *

#Initalizing the important Constants

START_DAY = date(year=2024, month=7, day=31)
SYMBOL = "DOGEUSD"
NUM_DAYS = 31

COLUMN_LIST = ['open', 'high', 'low', 'close', 'volume']

POLYGON_API_KEY = "d3tIobwoExakvIhj9u0XisNdsSt7n57O"
POLYGON_REST_BASEURL = "https://api.polygon.io"
SYMBOL_PREFIX = "X:"

TABLES_LIST = "crypto_daily", "crypto_minute", "crypto_all_minutes"

hostname = 'localhost'
database = 'db_crypto'
username = 'postgres'
pwd = '123456'
port_id = 5432

DATABASE_URL = f'postgresql+psycopg2://{username}:{pwd}@{hostname}:{port_id}/{database}'

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

    df_resampled = df.resample('min').asfreq()

    hourly_means = df.resample('h').mean()

    for hour, hour_group in df_resampled.groupby(df_resampled.index.floor('h')):
        df_resampled.loc[hour_group.index, COLUMN_LIST] = \
            hour_group[COLUMN_LIST].fillna(hourly_means.loc[hour])

    return df_resampled

# Backfilling the data

def generate_minute_data(df_daily, models):

    all_minutes = pd.date_range(df_daily.index.min(), df_daily.index.max(), freq='min')
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

def create_table(engine: Engine, table_name: str):
    inspector = inspect(engine)

    if table_name in inspector.get_table_names():
        print(f"Table '{table_name}' already exists!")
    else:
        metadata = MetaData()

        table = Table(
            table_name, metadata,
            Column('date', DateTime, primary_key=True),
            Column('open', Float, nullable=False),
            Column('high', Float, nullable=False),
            Column('low', Float, nullable=False),
            Column('close', Float, nullable=False),
            Column('volume', Float, nullable=False)
        )

        metadata.create_all(engine)
        print(f"Table '{table_name}' created successfully!")


def copy_from_dataframe(conn, df, table):
    buffer = StringIO()

    df.to_csv(buffer, sep='\t', header=False, index=True)

    buffer.seek(0)

    cur = conn.cursor()

    try:
        cur.copy_from(buffer, table, sep='\t', null='')
        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cur.close()




