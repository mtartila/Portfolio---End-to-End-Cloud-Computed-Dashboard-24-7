import os
import dash
from dash import dcc, html
import dash.dependencies as dd
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine

# Initialize the Dash app
app = dash.Dash(__name__)

# Fetch database credentials from environment variables
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

# Create the engine using pymysql
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

def fetch_data():
    # Execute a query and load the data into a DataFrame
    query = 'SELECT * FROM finnhub_stock_data'
    df = pd.read_sql(query, con=engine)

    # Ensure 'record_date' is in datetime format
    df['record_date'] = pd.to_datetime(df['record_date'])

    # Set 'record_date' as the index
    df.set_index('record_date', inplace=True)

    # Sort by 'record_date'
    df.sort_index(inplace=True)
    
    return df

# Create the layout of the Dash app
app.layout = html.Div(children=[
    html.H1('US Stock Price Chart'),
    html.Div('Latest close prices for several US stocks'),
    dcc.Graph(id='example'),
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # Update every 1 minute
        n_intervals=0
    )
])

# Update the graph periodically
@app.callback(
    dd.Output('example', 'figure'),
    [dd.Input('interval-component', 'n_intervals')]
)
def update_graph(n):
    df = fetch_data()
    fig = px.line(df, x=df.index, y='close', color='symbol', title='Stock Prices Over Time', markers=True)
    return fig

# For local development, run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True)

# Ensure the server is accessible to Gunicorn
server = app.server
