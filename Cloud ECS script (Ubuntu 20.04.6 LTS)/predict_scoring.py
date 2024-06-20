import joblib
import pandas as pd
import mysql.connector
import datetime
import pytz

# Loading the SARIMAX model
model_MSFT = joblib.load('sarimax_model_MSFT.pkl')
model_AAPL = joblib.load('sarimax_model_AAPL.pkl')
model_AMZN = joblib.load('sarimax_model_AMZN.pkl')
model_GOOGL = joblib.load('sarimax_model_GOOGL.pkl')

#AIVEN DB
host = ''
database = ''
user = ''
password = ''
port =  

def fetch_data():
    
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
    )
    query = 'SELECT symbol, record_date, daily_avg_open, daily_avg_high, daily_avg_low, daily_avg_close FROM finnhub_stock_data_daily'
    df = pd.read_sql(query, con=connection)
    df['record_date'] = pd.to_datetime(df['record_date'])
    connection.close()
    return df

df = fetch_data()
df = df.query('daily_avg_open.notna()')
symbols = [x for x in df['symbol'].unique()]

for x in symbols:
    globals()[f'df_{x}'] = df[df['symbol'] == x].groupby('record_date').sum()[['daily_avg_close']]
    globals()[f'df_{x}'] = globals()[f'df_{x}'].rename(columns={"daily_avg_close": f"close_{x}" })

df = pd.concat([df_AAPL, df_MSFT, df_AMZN, df_GOOGL], axis=1)


df = df.asfreq('B')  
df.fillna(method='ffill', inplace=True)


#stating exogenous and endogenous data for enriching model data
exog_MSFT = df[['close_AAPL', 'close_AMZN', 'close_GOOGL']].tail(1)
endog_MSFT = df[['close_MSFT']].tail(1)

exog_AAPL = df[['close_MSFT', 'close_AMZN', 'close_GOOGL']].tail(1)
endog_AAPL = df[['close_AAPL']].tail(1)

exog_AMZN = df[['close_MSFT', 'close_AAPL', 'close_GOOGL']].tail(1)
endog_AMZN = df[['close_AMZN']].tail(1)

exog_GOOGL = df[['close_MSFT', 'close_AAPL', 'close_AMZN']].tail(1)
endog_GOOGL = df[['close_GOOGL']].tail(1)

for x in symbols:
    try:
        globals()[f'model_{x}'] = globals()[f'model_{x}'].append(endog=globals()[f'endog_{x}'], exog=globals()[f'exog_{x}'])
    except Exception as e:
        print(f"Error appending endogenous data, no new record_date at {x}")

#Prediction scoring
forecast_steps = 1
for x in symbols:
    globals()[f'forecast_values_{x}'] = globals()[f'model_{x}'].forecast(steps=forecast_steps, exog=globals()[f'exog_{x}'])
    
for x in symbols:
    globals()[f'df_forecast_{x}'] = pd.DataFrame(globals()[f'forecast_values_{x}']).set_axis(['predicted_mean'], axis=1)

inserted_date = datetime.datetime.now()

try:
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
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
        sarimax_prediction FLOAT,
        transformed_date DATETIME,
        PRIMARY KEY (symbol, record_date)
    )
    """)

    # Ensure sarimax_prediction column is emptied before inserting latest prediction
    cursor.execute("""
    UPDATE finnhub_stock_data_daily
    SET sarimax_prediction = NULL
    """)

    # Insert latest SARIMAX predictions
    for x in symbols:
        for record_date, row in globals()[f'df_forecast_{x}'].iterrows():
            cursor.execute("""
                INSERT INTO finnhub_stock_data_daily (symbol, record_date, sarimax_prediction, transformed_date)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    sarimax_prediction = VALUES(sarimax_prediction),
                    transformed_date = VALUES(transformed_date)
            """, (x, record_date.date(), row['predicted_mean'], inserted_date))

        connection.commit()
        print("SARIMAX predictions inserted successfully")

except mysql.connector.Error as e:
    print(f"Database error: {e}")
finally:
    if connection:
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

joblib.dump(model_AAPL, 'sarimax_model_AAPL.pkl')
joblib.dump(model_AMZN, 'sarimax_model_AMZN.pkl')
joblib.dump(model_MSFT, 'sarimax_model_MSFT.pkl')
joblib.dump(model_GOOGL, 'sarimax_model_GOOGL.pkl')
print("Model saved successfully")
