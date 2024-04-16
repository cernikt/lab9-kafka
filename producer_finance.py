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
    parser.add_argument('dev', default=False)
    parser.add_argument('ticker', default='QQQ')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # read the ticker
    ticker = args.ticker
    print('ticker', ticker)

    # read the dev flag
    dev = args.dev
    print(dev)
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
    if dev:
        while True:

            # date range
            # sd = get_today()
            sd = get_date_from_string('2024-04-01')
            ed = sd + timedelta(days=1)
            # download data
            dfvp = yf.download(tickers=ticker, start=sd, end=ed, interval="1m")

            topic = ticker
            for index, row in dfvp.iterrows():

        
                message_dict = json.dumps({
                    'timestamp': str(index),
                    'open': str(row['Open']),
                    'high': str(row['High']),
                    'low': str(row['Low']),
                    'close': str(row['Close'])
                })

                print(index, "Open:", row['Open'], "Close:", row['Close'], "High:", row['High'], "Low:", row['Low']) # debug only

                print("before produce")
                producer.produce(topic=ticker, key=str(index), value=message_dict.encode('utf-8'), callback=delivery_callback)
                print("after produce")
                count += 1
                time.sleep(5)

            # Block until the messages are sent.
            producer.poll(10000)
            producer.flush()
    else:
        while True:
            # date range
            # Get the current date and time
            # current_datetime = datetime.now(market_timezone)

            # # Adjust the start date to the beginning of the current minute
            # sd = current_datetime - timedelta(seconds=current_datetime.second,
            #                                             microseconds=current_datetime.microsecond)
            sd = get_today()
            ed = sd + timedelta(days=1)
            # download data
            dfvp = yf.download(tickers=ticker, start=sd, interval="1m")

            if not dfvp.empty:
                latest_timestep = dfvp.index[-1]
                latest_row = dfvp.iloc[-1]
                # for index, row in dfvp.iterrows():
                message_dict = json.dumps({
                    'timestamp': str(latest_timestep),
                    'open': str(latest_row['Open']),
                    'high': str(latest_row['High']),
                    'low': str(latest_row['Low']),
                    'close': str(latest_row['Close'])
                })

                print(latest_timestep, "Open:", latest_row['Open'], "Close:", latest_row['Close'], "High:", latest_row['High'], "Low:", latest_row['Low']) # debug only

                producer.produce(topic=ticker, key=str(latest_timestep), value=message_dict.encode('utf-8'), callback=delivery_callback)

                count += 1
                time.sleep(5)
            else:
                print("Unable to fetch data from Yahoo Finance. waiting for 5 seconds...")
                time.sleep(5)


            # Block until the messages are sent.
            producer.poll(10000)
            producer.flush()
