import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime as dt

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

# Fetch initial data
df = fetch_data()

# Create the initial figure
initial_symbol = 'MSFT'
filtered_df = df[df['symbol'] == initial_symbol]
fig = px.line(filtered_df, x=filtered_df.index, y='close', title='Stock Prices Over Time', markers=True)
fig.update_traces(connectgaps=False)

# Define the layout of the Dash app
app.layout = html.Div(children=[
    html.Label('Dropdown'),
    dcc.Dropdown(id='symbol-picker',
                 options=[
                     {'label': 'Microsoft', 'value': 'MSFT'},
                     {'label': 'Apple', 'value': 'AAPL'},
                     {'label': 'Google', 'value': 'GOOGL'},
                     {'label': 'Amazon', 'value': 'AMZN'}
                 ],
                 multi=True,
                 value=['MSFT']),
    
    dcc.DatePickerRange(
        id='date-picker-range',
        calendar_orientation='horizontal',
        day_size=39,
        end_date_placeholder_text="Return",
        with_portal=False,
        first_day_of_week=0,
        reopen_calendar_on_clear=True,
        is_RTL=False,
        clearable=True,
        number_of_months_shown=1,
        min_date_allowed=df.index[0].date(),
        max_date_allowed=df.index[-1].date()
    ),
    
    html.H1('US Stock Price Chart', style={'textAlign': 'center'}),
    html.Div('Latest close prices for several US stocks'),
    dcc.Graph(id='example', figure=fig),
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # Update every 1 minute
        n_intervals=0
    )
])

# Define callback to update graph based on dropdown selection, date range, and periodic updates
@app.callback(
    Output('example', 'figure'),
    [Input('symbol-picker', 'value'),
     Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date'),
     Input('interval-component', 'n_intervals')]
)
def update_figure(selected_symbols, start_date, end_date, n_intervals):
    print(f"Selected symbols: {selected_symbols}, Start date: {start_date}, End date: {end_date}")  # Debug statement
    df = fetch_data()  # Fetch the latest data
    filtered_df = df.loc[df['symbol'].isin(selected_symbols)]
    
    if start_date:
        filtered_df = filtered_df.loc[filtered_df.index >= pd.to_datetime(start_date)]
    if end_date:
        filtered_df = filtered_df.loc[filtered_df.index <= pd.to_datetime(end_date)]
    
    fig = px.line(filtered_df, x=filtered_df.index, y='close', color='symbol', title=f'Stock Prices Over Time: {selected_symbols}', markers=True)
    fig.update_traces(connectgaps=False)  # Ensure connectgaps is set to False
    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)

# Ensure the server is accessible to Gunicorn
server = app.server
