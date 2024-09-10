from sqlalchemy import create_engine
from crypto_data import df_daily, df_all_minutes, df_minute

hostname = 'localhost'
database = 'db_crypto'
username = 'postgres'
pwd = '123456'
port_id = 5432

DATABASE_URL = f'postgresql+psycopg2://{username}:{pwd}@{hostname}:{port_id}/{database}'

engine = create_engine(DATABASE_URL)


df_daily.to_sql(
    name="crypto_daily",
    con=engine,
    if_exists="append",
    index=True
)