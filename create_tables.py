from sqlalchemy import create_engine, Column, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from crypto_data import df_daily, df_minute, df_all_minutes

hostname = 'localhost'
database = 'db_crypto'
username = 'postgres'
pwd = '123456'
port_id = 5432

DATABASE_URL = f'postgresql+psycopg2://{username}:{pwd}@{hostname}:{port_id}/{database}'

engine = create_engine(DATABASE_URL)

Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

def create_table(table_name):
    class CryptoData(Base):
        __tablename__ = table_name

        date = Column(DateTime, primary_key=True)
        open = Column(Float, nullable=False)
        high = Column(Float, nullable=False)
        low = Column(Float, nullable=False)
        close = Column(Float, nullable=False)
        volume = Column(Float, nullable=False)

    Base.metadata.create_all(engine)
    return CryptoData

def insert_data(df, table_class):
    for index, row in df.iterrows():
        data_row = table_class(
            date=index,
            open=row['open'],
            high=row['high'],
            low=row['low'],
            close=row['close'],
            volume=row['volume']
        )
        session.add(data_row)
    session.commit()

table_minute = create_table('crypto_minute')
table_daily = create_table('crypto_daily')
table_all_minutes = create_table('crypto_all_minutes')

insert_data(df_minute, table_minute)
insert_data(df_daily, table_daily)
insert_data(df_all_minutes, table_all_minutes)

print("Tables created and data inserted successfully!")
