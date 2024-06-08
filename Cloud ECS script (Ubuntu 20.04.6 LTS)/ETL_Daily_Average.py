import pandas as pd
import mysql.connector
import datetime

# Database credentials
username = 'your_username'
password = 'your_password'
host = 'your_host'
port = 'your_port'
database = 'your_database'

def fetch_data():
    # Connect to the database
    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=username,
        password=password,
        port=port  # Ensure port is specified correctly
    )
    query = 'SELECT * FROM finnhub_stock_data'
    df = pd.read_sql(query, con=connection)
    df['record_date'] = pd.to_datetime(df['record_date'])
    connection.close()
    return df

def transform_data(df):
    df['record_date'] = df['record_date'].dt.date
    daily_avg = df.groupby(['record_date', 'symbol'])[['close','open','high','low']].mean().reset_index()
    daily_avg.rename(columns={'close': 'daily_avg_close', 'open': 'daily_avg_open', 'high': 'daily_avg_high', 'low': 'daily_avg_low'}, inplace=True)
    return daily_avg

df = fetch_data()
df = transform_data(df)

inserted_date = datetime.datetime.now()

try:
    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=username,
        password=password,
        port=int(port)  # Ensure port is an integer
    )

    cursor = connection.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS finnhub_stock_data_daily (
        symbol VARCHAR(10),
        record_date DATE,
        daily_avg_open FLOAT,
        daily_avg_high FLOAT,
        daily_avg_low FLOAT,
        daily_avg_close FLOAT,
        transformed_date DATETIME,
        PRIMARY KEY (symbol, record_date)
    )
    """)

    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO finnhub_stock_data_daily (symbol, record_date, daily_avg_open, daily_avg_high, daily_avg_low, daily_avg_close, transformed_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                daily_avg_open = VALUES(daily_avg_open),
                daily_avg_high = VALUES(daily_avg_high),
                daily_avg_low = VALUES(daily_avg_low),
                daily_avg_close = VALUES(daily_avg_close),
                transformed_date = VALUES(transformed_date)
        """, (row['symbol'], row['record_date'], row['daily_avg_open'], row['daily_avg_high'], row['daily_avg_low'], row['daily_avg_close'], inserted_date))

    connection.commit()
    print("All data inserted successfully")
            
except mysql.connector.Error as e:
    print(f"Error: {e}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
