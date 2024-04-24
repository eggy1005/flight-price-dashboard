import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json

def draw():
    st.set_page_config(
        page_title="Real-Time 🍊Travel buddy Dashboard",
        page_icon="✅",
        layout="wide",
    )

    st.title("Real-Time 🍊Travel buddy Dashboard")
    # job_filter = st.selectbox("Select destination", pd.unique(df["ARRIVAL"]))
    # # dataframe filter
    # df = df[df["TOTAL_PRICE"] == job_filter]
    consume_data()


def consume_data():
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        'flight_hotel_price_combined',
        bootstrap_servers=['pkc-41wq6.eu-west-2.aws.confluent.cloud:9092'],
        auto_offset_reset='earliest',
        group_id='flight-hotel-price-combined-2',
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username='7AICBYXFYBHMF5PJ',  # Replace with your SASL username
        sasl_plain_password='yylYReFtxTnInxAeng6ubVTs+sMXGBi8Hz0AdAIO4eVB8/LXJHxKASyh51B0eWu8'
    )

    # consumer.seek_to_beginning(consumer.partitions_for_topic('flight_hotel_price_combined'))

    # Initialize a list to store the messages
    # Collect messages
    placeholder = st.empty()
    data_frame = pd.DataFrame([], columns=['ARRIVAL_DATE', 'TOTAL_PRICE'])

    try:
        messages = []
        for message in consumer:
            # Decode the message and append to the list
            decoded_message = message.value.decode('utf-8')
            messages.append(json.loads(decoded_message))
            # For demonstration, let's assume we stop after collecting 10 messages
            print('i am consuming a message')
            if len(messages) > 10000:
                temp = pd.DataFrame(messages, columns=['ARRIVAL_DATE', 'TOTAL_PRICE'])
                data_frame = pd.concat([data_frame, temp], ignore_index=True)
                messages = []
                with placeholder.container():
                    st.line_chart(data_frame, x='ARRIVAL_DATE', y = 'TOTAL_PRICE')

        # if len(messages) >= 10:
            #     break
    finally:
        # Always close the consumer gently
        consumer.close()
    # data = [json.loads(s) for s in messages]
    # Convert the list of messages to a DataFrame
    # df = pd.DataFrame(data, columns=['ARRIVAL_DATE', 'TOTAL_PRICE'])
    print('Start consuming messages')
    # Display the DataFrame
    # return df

if __name__ == '__main__':
    draw()