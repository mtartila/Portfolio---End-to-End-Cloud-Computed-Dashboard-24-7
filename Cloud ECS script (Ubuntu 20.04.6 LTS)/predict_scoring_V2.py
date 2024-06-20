import joblib
import pandas as pd
import mysql.connector
import datetime
import logging

# Set up logging to ensure troubleshoot since scoring script have higher rate of errors
logging.basicConfig(level=logging.DEBUG, filename='script.log', 
                    format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Loading the SARIMAX models
    model_MSFT = joblib.load('sarimax_model_MSFT.pkl')
    model_AAPL = joblib.load('sarimax_model_AAPL.pkl')
    model_AMZN = joblib.load('sarimax_model_AMZN.pkl')
    model_GOOGL = joblib.load('sarimax_model_GOOGL.pkl')
    logging.info("Models loaded successfully")
except Exception as e:
    logging.error(f"Error loading models: {e}")

#AIVEN DB 
host = ''
database = ''
user = ''
password = ''
port =   

def fetch_data():
    try:
        
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
        logging.info("Data fetched successfully from the database")
        return df
    except mysql.connector.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Error fetching data: {e}")

df = fetch_data()
if df is not None:
    try:
        df = df.query('daily_avg_open.notna()')
        symbols = [x for x in df['symbol'].unique()]

        for x in symbols:
            globals()[f'df_{x}'] = df[df['symbol'] == x].groupby('record_date').sum()[['daily_avg_close']]
            globals()[f'df_{x}'] = globals()[f'df_{x}'].rename(columns={"daily_avg_close": f"close_{x}" })

        df = pd.concat([df_AAPL, df_MSFT, df_AMZN, df_GOOGL], axis=1)
        df = df.asfreq('B')
        df.fillna(method='ffill', inplace=True)
        logging.info("Data processing completed successfully")
    except Exception as e:
        logging.error(f"Error processing data: {e}")

    try:
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
                logging.error(f"Error appending endogenous data for {x}: {e}")
        
        
        exog_MSFT_predict = df[['close_AAPL','close_AMZN','close_GOOGL']].tail(5)
        exog_AAPL_predict = df[['close_MSFT','close_AMZN','close_GOOGL']].tail(5)
        exog_AMZN_predict = df[['close_MSFT','close_AAPL','close_GOOGL']].tail(5)
        exog_GOOGL_predict = df[['close_MSFT','close_AAPL','close_AMZN']].tail(5)
        # Make predictions
        
        forecast_steps = 5
        for x in symbols:
            globals()[f'forecast_values_{x}'] = globals()[f'model_{x}'].forecast(steps=forecast_steps, exog=globals()[f'exog_{x}_predict'])
        
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

            # Ensure sarimax_prediction column is emptied before inserting new prediction values
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
                    """, (x, record_date.date(), float(row['predicted_mean']), inserted_date))

            connection.commit()
            logging.info("SARIMAX predictions inserted successfully")

        except mysql.connector.Error as e:
            logging.error(f"Database error during insertion: {e}")
        finally:
            if connection:
                cursor.close()
                connection.close()
                logging.info("MySQL connection is closed")

        # Save the models
        joblib.dump(model_AAPL, 'sarimax_model_AAPL.pkl')
        joblib.dump(model_AMZN, 'sarimax_model_AMZN.pkl')
        joblib.dump(model_MSFT, 'sarimax_model_MSFT.pkl')
        joblib.dump(model_GOOGL, 'sarimax_model_GOOGL.pkl')
        logging.info("Models saved successfully")

    except Exception as e:
        logging.error(f"Error during prediction and insertion: {e}")
else:
    logging.error("Dataframe is None, skipping processing and prediction steps")
