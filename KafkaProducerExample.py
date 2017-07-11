import logging

from kafka import KafkaProducer
from time import time


"""
 Demo for kafka producer
"""
def main():
    logging.basicConfig(level=logging.DEBUG)
    message = '{}{}'.format('some_message_bytes_', time())
    producer = KafkaProducer(bootstrap_servers=['10.1.10.62:9092','10.1.10.63:9092','10.1.10.64:9092'])
    future = producer.send('my-topic', bytes(message, 'utf-8') )
    result = future.get(timeout=60)
if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()