import requests
import mysql.connector
from mysql.connector import Error
import datetime

# Finnhub API details
API_KEY = ' '
SYMBOLS = ['MSFT', 'AAPL', 'AMZN', 'GOOGL']  # List of stock symbols

# MySQL database details
host = 'mysql-tartilaportfolio-mtartila-project-portfolio.g.aivencloud.com'
database = ''
user = ''
password = ''
port = ''  # Custom port for Aiven MySQL

try:
    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port  # Specify the custom port here
    )

    if connection.is_connected():
        cursor = connection.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS finnhub_stock_data (
            symbol VARCHAR(10),
            record_date DATETIME,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            previous_close FLOAT,
            inserted_date DATETIME,
            PRIMARY KEY (symbol, record_date)
        )
        """)

        for symbol in SYMBOLS:
            URL = f'https://finnhub.io/api/v1/quote?symbol={symbol}&token={API_KEY}'
            response = requests.get(URL)
            data = response.json()

            # Debug: Print the received data
            print("API Response:", data)

            if 'c' in data:
                current_time = datetime.datetime.now()
                open_price = data.get('o', 0)
                high_price = data.get('h', 0)
                low_price = data.get('l', 0)
                close_price = data.get('c', 0)
                previous_close = data.get('pc', 0)
                inserted_date = datetime.datetime.now()

                # Debug: Print the data to be inserted
                print(f"Inserting data for {symbol} at {current_time}: open={open_price}, high={high_price}, low={low_price}, close={close_price}, previous_close={previous_close}, inserted_date={inserted_date}")

                try:
                    cursor.execute("""
                    INSERT INTO finnhub_stock_data (symbol, record_date, open, high, low, close, previous_close, inserted_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        previous_close = VALUES(previous_close),
                        inserted_date = VALUES(inserted_date)
                    """, (symbol, current_time, open_price, high_price, low_price, close_price, previous_close, inserted_date))
                except Error as e:
                    print(f"SQL Error: {e}")

        connection.commit()
        print("All data inserted successfully")

except Error as e:
    print(f"Error: {e}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
