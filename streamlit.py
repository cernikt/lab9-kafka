#!/usr/bin/env python

import streamlit as st
import pandas as pd
import yfinance as yf
from configparser import ConfigParser
import time
import asyncio
from confluent_kafka import Producer, Consumer, OFFSET_BEGINNING
import os
import json

if not os.path.exists("received_messages"):
    os.makedirs("received_messages")

async def setup_consumer(ticker, df, consumer):
    topics = [ticker]

    def reset_offset(consumer, partitions):
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

    consumer.subscribe(topics, on_assign=reset_offset)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                await asyncio.sleep(1)  # Wait for new messages
                continue
            elif msg.error():
                print("ERROR: {}".format(msg.error()))
                continue
            else:

                with open(f"received_messages/{ticker}_data.txt", "a") as file:
                    file.write("value = {}\n".format(msg.value()))

                json_output = json.loads(msg.value().decode('utf-8'))

                print("successfully parsed json")

                # print("Consumed event from topic {}: key = {} value = {}".format(
                #     msg.topic(), msg.key().decode('utf-8'), msg.value().decode('utf-8'))
                # )

                with open(f"received_messages/{ticker}_message.txt", "a") as file:
                    file.write("Consumed event from topic {}: key = {} value = {}\n".format(
                        msg.topic(), msg.key().decode('utf-8'), msg.value().decode('utf-8')))
                
                new_df = pd.read_json(msg.value().decode('utf-8'))

                # Update the existing DataFrame or create a new one
                if df is None:
                    df = new_df
                else:
                    df = pd.concat([df, new_df])  # Append new data
                # Process the updated DataFrame (e.g., visualize)
                # For Streamlit, you might update the UI with the new data
                # print("checking if possible to generate chart")
                # if df is not None and not df.empty:
                    # print("generating chart")
                st.write('Price Chart')
                price_chart = pd.DataFrame({
                    'Open': df['Open'],
                    'Close': df['Close'],
                    'High': df['High'],
                    'Low': df['Low']
                })
                st.line_chart(price_chart)
    except KeyboardInterrupt:
        pass  # Handle keyboard interrupt gracefully
    finally:
        consumer.close()


async def main():
    st.title("Stock Data Visualization")

    st.sidebar.header('Select Options')
    option = st.sidebar.selectbox('Select one symbol', ('AAPL', 'MSFT', "SPY", 'WMT'))
    today = pd.Timestamp.now().date()
    before = today - pd.Timedelta(days=700)
    start_date = st.sidebar.date_input('Start date', before)
    end_date = st.sidebar.date_input('End date', today)

    if start_date < end_date:
        st.sidebar.success('Start date: `%s`\n\nEnd date: `%s`' % (start_date, end_date))
    else:
        st.sidebar.error('Error: End date must fall after start date.')

    config_parser = ConfigParser()
    with open("getting_started.ini") as f:
        config_parser.read_file(f)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    consumer = Consumer(config)

    df = None  # Initialize DataFrame
    consumer_task = asyncio.create_task(setup_consumer(option, df, consumer))  # Start consumer as a task

    # This loop keeps Streamlit running until the task completes or user interrupts
    while consumer_task.done() is False:
        await asyncio.sleep(1)  # Keep Streamlit responsive
    else:
        await consumer_task  # Wait for the consumer task to complete

    # print("checking if possible to generate chart")
    # if not df == None and not df.empty:
    #     print("generating chart")
    #     st.write('Price Chart')
    #     price_chart = pd.DataFrame({
    #         'Open': df['Open'],
    #         'Close': df['Close'],
    #         'High': df['High'],
    #         'Low': df['Low']
    #     })
    #     st.line_chart(price_chart)

if __name__ == "__main__":
    asyncio.run(main())

