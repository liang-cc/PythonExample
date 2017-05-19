from kafka import KafkaProducer
from kafka import KafkaConsumer
import threading, logging, time
"""
 Demo for kafka consumer
"""
def main():
    consumer = KafkaConsumer(bootstrap_servers=['10.1.10.62:9092', '10.1.10.63:9092', '10.1.10.64:9092'])
    consumer.subscribe(['my-topic'])
    for message in consumer:
        print(message)
if __name__ == '__main__':
    main()