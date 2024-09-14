from crypto_data import *

def main():
    df_minute = collect_historical_data(SYMBOL, START_DAY, NUM_DAYS)
    df_minute = fill_missing_values(df_minute)

    df_daily = create_daily_df("DOGE-USD")

    df_all_minutes = generate_minute_data(df_daily, df_minute)

    create_tables()
    insert_data_in_table([df_minute,df_daily,df_all_minutes])


if __name__ == '__main__':
    main()