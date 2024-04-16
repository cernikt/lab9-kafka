from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
import streamlit as st
import configparser

# Function to update DataFrame with data from Kafka
def update_df_from_kafka(df, message):
    # Process the message received from Kafka (Assuming message is in JSON format)
    data = json.loads(message.value().decode('utf-8'))
    # Update the DataFrame accordingly
    # Example: df = df.append(data, ignore_index=True)
    return df.append(data, ignore_index=True)

# Function to create price chart
def create_price_chart(df):
    price_chart = pd.DataFrame({
        'Open': df['Open'],
        'Close': df['Close'],
        'High': df['High'],
        'Low': df['Low']
    })
    return st.line_chart(price_chart)

# Read Kafka consumer configuration from file
config_parser = configparser.ConfigParser()
config_parser.read("getting_started.ini")
config = dict(config_parser['default'])
config.update(config_parser['consumer'])

# Create Kafka consumer
consumer = Consumer(config)

# Set up initial DataFrame
df = pd.DataFrame()

# Main Streamlit app
st.title('Kafka Consumer App')

# Streamlit sidebar
topic_name = st.sidebar.selectbox('Select Kafka Topic', ('AAPL', 'MSFT', 'SPY', 'WMT'))

# Subscribe to Kafka topic
consumer.subscribe([topic_name])

# Create an initial chart
chart_placeholder = st.empty()
price_chart = None

# Start Kafka consumer loop
try:
    while True:
        message = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, the consumer reached the end of the log
                continue
            else:
                # Other errors
                print(message.error())
                break

        df = update_df_from_kafka(df, message)

        # If DataFrame is not empty, update price chart
        print("before generation of chart")
        if not df.empty:
            if price_chart is None:
                price_chart = create_price_chart(df)
            else:
                price_chart.line_chart(pd.DataFrame({
                    'Open': df['Open'],
                    'Close': df['Close'],
                    'High': df['High'],
                    'Low': df['Low']
                }))

        # Update Streamlit UI
        st.write('Updated DataFrame:')
        st.dataframe(df)

except KeyboardInterrupt:
    consumer.close()
