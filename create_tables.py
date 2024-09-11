from crypto_data import *

def main():
    df_minute = collect_historical_data(SYMBOL, START_DAY, NUM_DAYS)
    df_minute = fill_missing_values(df_minute)

    df_daily = create_daily_df("DOGE-USD")

    df_all_minutes = generate_minute_data(df_daily, df_minute)

    DF_LIST = df_daily, df_minute, df_all_minutes

    conn = psycopg2.connect(
        host=hostname,
        database=database,
        user=username,
        password=pwd
    )

    engine = create_engine(DATABASE_URL)

    for table in TABLES_LIST:
        create_table(engine, table)
    for df,table in zip(DF_LIST,TABLES_LIST):
        copy_from_dataframe(conn, df, table)
    conn.close()
if __name__ == '__main__':
    main()