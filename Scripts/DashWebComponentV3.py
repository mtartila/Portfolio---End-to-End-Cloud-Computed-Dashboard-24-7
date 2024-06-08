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

def fetch_data(query):
    # Execute a query and load the data into a DataFrame
    df = pd.read_sql(query, con=engine)
    df['record_date'] = pd.to_datetime(df['record_date'])
    df.set_index('record_date', inplace=True)
    df.sort_index(inplace=True)
    return df

# Fetch initial data
df = fetch_data('SELECT * FROM finnhub_stock_data')
df_daily = fetch_data('SELECT * FROM finnhub_stock_data_daily')

# Create the initial figure
initial_symbol = 'MSFT'
initial_symbol_2 = 'MSFT'
filtered_df = df[df['symbol'] == initial_symbol]
filtered_df_daily = df_daily[df_daily['symbol'] == initial_symbol_2]
newnames = {'daily_avg_open':'Open', 'daily_avg_high': 'High', 'daily_avg_low' : 'Low','daily_avg_close':'Close'}

fig = px.line(filtered_df, x=filtered_df.index, y='close', title='Stock Prices Over Time', markers=True)
fig.update_traces(connectgaps=False)
fig.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font=dict(color='white'),
    title_font=dict(color='white'),
    legend_title_font=dict(color='white')
)

fig2 = px.line(filtered_df_daily, x=filtered_df_daily.index, y=['daily_avg_open','daily_avg_high','daily_avg_low','daily_avg_close'], title=f'Daily Stock Price: {initial_symbol_2}', markers=True)
fig2.update_traces(connectgaps=False)
fig2.for_each_trace(lambda t: t.update(name=newnames[t.name],
                                       legendgroup=newnames[t.name],
                                       hovertemplate=t.hovertemplate.replace(t.name, newnames[t.name])
                                      ))
fig2.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font=dict(color='white'),
    title_font=dict(color='white'),
    legend_title_font=dict(color='white')
)

# Define the layout of the Dash app
app.layout = html.Div(children=[
    html.Label('Stock Options'),
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
    dcc.Graph(id='figure1', figure=fig),
    
    html.Div(children=[
        html.Label('Detailed Stock Options'),
        dcc.Dropdown(id='symbol-picker-2',
                     options=[
                         {'label': 'Microsoft', 'value': 'MSFT'},
                         {'label': 'Apple', 'value': 'AAPL'},
                         {'label': 'Google', 'value': 'GOOGL'},
                         {'label': 'Amazon', 'value': 'AMZN'}
                     ],
                     value='MSFT'),
        dcc.Graph(id='figure2', figure=fig2)
    ]),
    
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # Update every 1 minute
        n_intervals=0
    )
])

# Define callback to update graphs based on dropdown selection, date range, and periodic updates
@app.callback(
    [Output('figure1', 'figure'),
     Output('figure2', 'figure')],
    [Input('symbol-picker', 'value'),
     Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date'),
     Input('symbol-picker-2', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_figure(selected_symbol, start, end, symbol2, n_intervals):
    print(f"Selected symbols: {selected_symbol}, Start date: {start}, End date: {end}, Interval: {n_intervals}")  # Debug statement
    
    # Ensure selected_symbol and symbol2 are lists
    if isinstance(selected_symbol, str):
        selected_symbol = [selected_symbol]
    if isinstance(symbol2, str):
        symbol2 = [symbol2]
    
    # Fetch latest data
    df = fetch_data('SELECT * FROM finnhub_stock_data')
    df_daily = fetch_data('SELECT * FROM finnhub_stock_data_daily')
    
    filtered_df = df.loc[df['symbol'].isin(selected_symbol)]
    filtered_df_daily = df_daily[df_daily['symbol'].isin(symbol2)]
    
    if start:
        filtered_df = filtered_df.loc[filtered_df.index >= pd.to_datetime(start)]
    if end:
        filtered_df = filtered_df.loc[filtered_df.index <= pd.to_datetime(end)]
    
    fig = px.line(filtered_df, x=filtered_df.index, y='close', color='symbol', title=f'Stock Prices Over Time: {selected_symbol}', markers=True)
    fig.update_traces(connectgaps=False)
    fig.update_layout(
        plot_bgcolor='black',
        paper_bgcolor='black',
        font=dict(color='white'),
        title_font=dict(color='white'),
        legend_title_font=dict(color='white')
    )
    
    fig2 = px.line(filtered_df_daily, x=filtered_df_daily.index, y=['daily_avg_open', 'daily_avg_high', 'daily_avg_low', 'daily_avg_close'], title=f'Daily Stock Price: {symbol2}', markers=True)
    fig2.update_traces(connectgaps=False)
    fig2.for_each_trace(lambda t: t.update(name=newnames[t.name],
                                           legendgroup=newnames[t.name],
                                           hovertemplate=t.hovertemplate.replace(t.name, newnames[t.name])
                                          ))
    fig2.update_layout(
        plot_bgcolor='black',
        paper_bgcolor='black',
        font=dict(color='white'),
        title_font=dict(color='white'),
        legend_title_font=dict(color='white')
    )
    
    return fig, fig2

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)

# Ensure the server is accessible to Gunicorn
server = app.server
