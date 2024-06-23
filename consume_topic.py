import json
import argparse

from kafka import KafkaConsumer, TopicPartition
from consts import BOOTSTRAP_SERVERS, TEST_TOPIC

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--partition', type=int)

    args = parser.parse_args()
    partition = args.partition

    if partition is not None:
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            auto_offset_reset='earliest',
        )

        consumer.assign([TopicPartition(TEST_TOPIC, partition)])
    else:
        consumer = KafkaConsumer(
            TEST_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            auto_offset_reset='earliest',
        )

    for message in consumer:
        print(f'{message.value} on partition {message.partition}')
