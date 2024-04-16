# !pip install streamlit
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yfinance as yf # https://pypi.org/project/yfinance/
from ta.volatility import BollingerBands
from ta.trend import MACD
from ta.momentum import RSIIndicator

##################
# Set up sidebar #
##################

# Add in location to select image.

option = st.sidebar.selectbox('Select one symbol', ( 'AAPL', 'MSFT',"SPY",'WMT'))


import datetime

today = datetime.date.today()
before = today - datetime.timedelta(days=700)
start_date = st.sidebar.date_input('Start date', before)
end_date = st.sidebar.date_input('End date', today)
if start_date < end_date:
    st.sidebar.success('Start date: `%s`\n\nEnd date:`%s`' % (start_date, end_date))
else:
    st.sidebar.error('Error: End date must fall after start date.')


##############
# Stock data #
##############

# https://technical-analysis-library-in-python.readthedocs.io/en/latest/ta.html#momentum-indicators

df = yf.download(option,start= start_date,end= end_date, progress=False)


###################
# Set up main app #
###################

progress_bar = st.progress(0)

# https://share.streamlit.io/daniellewisdl/streamlit-cheat-sheet/app.py


st.write('Price Chart')
price_chart = pd.DataFrame({
    'Open': df['Open'],
    'Close': df['Close'],
    'High': df['High'],
    'Low': df['Low']
})
st.line_chart(price_chart)


st.write('Recent data ')
st.dataframe(df.tail(10))










