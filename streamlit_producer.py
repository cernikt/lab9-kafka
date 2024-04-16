#!/usr/bin/env python

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

import requests
import json
import pytz
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import date
from datetime import timedelta
import time
import yfinance as yf

def is_data_available(ticker, start_date, end_date):
    """
    Check if historical data is available for the specified ticker symbol and date range.

    Args:
    - ticker (str): Ticker symbol.
    - start_date (str): Start date in YYYY-MM-DD format.
    - end_date (str): End date in YYYY-MM-DD format.

    Returns:
    - bool: True if data is available, False otherwise.
    """
    try:
        # Create Ticker object
        ticker_obj = yf.Ticker(ticker)

        # Get historical data
        historical_data = ticker_obj.history(start=start_date, interval="1m")

        # Check if data is available
        if not historical_data.empty:
            return True
        else:
            return False

    except Exception as e:
        print(f"Error checking data availability: {e}")
        return False


def get_today():
    return date.today()

def get_next_day_str(today):
    return get_date_from_string(today) + timedelta(days=1)

def get_date_from_string(expiration_date):
    return datetime.strptime(expiration_date, "%Y-%m-%d").date()

def get_string_from_date(expiration_date):
    return expiration_date.strftime('%Y-%m-%d')

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('ticker', default='SPY')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # read the ticker
    ticker = args.ticker
    print('ticker', ticker)

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    count = 0
    # Produce data by repeatedly fetching today's stock prices - feel free to change
    while True:
        # date range
        # Get the current date and time
        # current_datetime = datetime.now(market_timezone)

        # # Adjust the start date to the beginning of the current minute
        # sd = current_datetime - timedelta(seconds=current_datetime.second,
        #                                             microseconds=current_datetime.microsecond)

        latest_trading = yf.download(ticker, period="1m")


        sd = get_today()
        ed = sd + timedelta(days=1)
        # download data
        dfvp = yf.download(tickers=ticker, start=sd, interval="1m")

        if not dfvp.empty:

            latest_timestep = dfvp.index[-1]
            latest_row = dfvp.iloc[-1]

            # message_dict = json.dumps({
            #     'timestamp': str(latest_timestep),
            #     'open': str(latest_trading['Open']),
            #     'high': str(latest_trading['High']),
            #     'low': str(latest_trading['Low']),
            #     'close': str(latest_trading['Close'])
            # })    
            producer.produce(topic=ticker, key=str(latest_timestep), value=dfvp.to_json().encode('utf-8'), callback=delivery_callback)

            count += 1
            time.sleep(5)
        else:
            print("Unable to fetch data from Yahoo Finance. waiting for 5 seconds...")
            time.sleep(5)


        # Block until the messages are sent.
        producer.poll(10000)
        producer.flush()
