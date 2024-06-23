from kafka.admin import KafkaAdminClient, NewTopic
from consts import BOOTSTRAP_SERVERS, CLIENT_ID, TEST_TOPIC


if __name__ == '__main__':
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id=CLIENT_ID)
        admin_client.create_topics(new_topics=[
            NewTopic(
                name=TEST_TOPIC,
                num_partitions=2,
                replication_factor=1)
            ],
            validate_only=False)
        print('Create successfully')
    except Exception as e:
        print(e)
        print('Topic creation failed')
    finally:
        admin_client.close()
