import json
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from consts import BOOTSTRAP_SERVERS, CLIENT_ID, TEST_TOPIC


def get_kafka_producer(bootstrap_servers: list[str], retries: int = 5, delay: int = 30, value_serializer=None):
    for retry in range(retries):
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=CLIENT_ID, max_block_ms=5000,
                                     value_serializer=value_serializer)
            return producer
        except NoBrokersAvailable as e:
            print(f'Attempt {retry}/{retries} to get Kafka broker failed: {e}')
            time.sleep(delay)
    raise Exception(f'Failed to connect to Kafka after {retries} retries')


def get_partition(message, key):
    first_letter = message[key][0].lower()
    return 0 if first_letter < 'n' else 1


if __name__ == '__main__':
    producer = get_kafka_producer(BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    users = [
        {'username': 'mario', 'age': 29},
        {'username': 'zippa', 'age': 35},
        {'username': 'buck', 'age': 50}
    ]
    for user in users:
        partition = get_partition(user, 'username')
        producer.send(topic=TEST_TOPIC, partition=partition, value=user)

    producer.close()
