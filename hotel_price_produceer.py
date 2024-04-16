import argparse
import atexit
import json
import logging
import random
import time
import sys

from confluent_kafka import Producer

logging.basicConfig(
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("flight_producer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger()

HOTEL_CITY = ['PARIS', 'BARCELONA', 'DUBAI', 'CAIRO']
USD = 'USD'


class ProducerCallback:
    def __init__(self, record, log_success=False):
        self.record = record
        self.log_success = log_success

    def __call__(self, err, msg):
        if err:
            logger.error('Error producing record {}'.format(self.record))
        elif self.log_success:
            logger.info('Produced {} to topic {} partition {} offset {}'.format(
                self.record,
                msg.topic(),
                msg.partition(),
                msg.offset()
            ))


def main(args):
    logger.info('Starting flight producer')
    conf = {
        'bootstrap.servers': args.bootstrap_server,
        'linger.ms': 200,
        'client.id': 'hotel-price-usd'
    }

    producer = Producer(conf)

    atexit.register(lambda p: p.flush(), producer)

    i = 1
    while True:
        is_tenth = i % 10 == 0

        hotel_price_usd = {
            'CITY': random.choice(HOTEL_CITY),
            'PRICE_USD': random.randrange(100, 1000),
            'CURRENCY': USD
        }
        producer.produce(topic=args.topic,
                         value=json.dumps(hotel_price_usd),
                         on_delivery=ProducerCallback(hotel_price_usd, log_success=is_tenth))

        if is_tenth:
            producer.poll(1)
            time.sleep(5)
            i = 0  # no need to let i grow unnecessarily large

        i += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', default='localhost:9092')
    parser.add_argument('--topic', default='hotel-price-usd')
    args = parser.parse_args()
    main(args)
    #  flight-price-gbp
    #  flight-price-average-usd
