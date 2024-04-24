import pandas as pd
from kafka import KafkaConsumer
def consume_data():
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        'flight_hotel_price_combined',
        bootstrap_servers=['pkc-41wq6.eu-west-2.aws.confluent.cloud:9092'],
        auto_offset_reset='earliest',
        group_id='flight-hotel-price-combined',
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username='7AICBYXFYBHMF5PJ',  # Replace with your SASL username
        sasl_plain_password='yylYReFtxTnInxAeng6ubVTs+sMXGBi8Hz0AdAIO4eVB8/LXJHxKASyh51B0eWu8'
    )

    # Initialize a list to store the messages
    messages = []
    # Collect messages
    try:
        for message in consumer:
            # Decode the message and append to the list
            decoded_message = message.value.decode('utf-8')
            messages.append(decoded_message)

            # For demonstration, let's assume we stop after collecting 10 messages
            if len(messages) >= 10:
                break
    finally:
        # Always close the consumer gently
        consumer.close()

    # Convert the list of messages to a DataFrame
    df = pd.DataFrame(messages, columns=['Message'])
    print('Start consuming messages')
    # Display the DataFrame
    print(df)
    return df
if __name__ == '__main__':
    consume_data()