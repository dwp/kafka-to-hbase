import happybase
import os

from kafka import KafkaProducer, KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS"
) or "localhost:9092"

HBASE_HOST = os.environ.get(
    "HBASE_HOST"
) or "localhost:9090"


def create_kafka_producer():
    """ Create a Kafka producer instance """
    return KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)


def create_kafka_consumer(consumer_group, topics):
    """ Create a Kafka consumer instance """
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=consumer_group,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        api_version=(0, 9),
    )

    consumer.subscribe(topics)
    return consumer


def create_hbase_client():
    """ Create an Hbase client instance """
    parts = HBASE_HOST.split(":", 1) + ["9090"]
    host = parts[0]
    port = int(parts[1])

    return happybase.Connection(host, port)
