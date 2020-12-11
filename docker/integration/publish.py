from time import time

import util
from kafka import KafkaProducer

logger = util.get_logger(__name__)

topic_count = 10
records_per_topic = 1000


def publish_records():
    producer = KafkaProducer(bootstrap_servers='kafka:9092')

    logger.info("Starting kafka record publisher...")

    for collection_num in range(1, topic_count + 1):
        topic = util.topic_name(collection_num)
        excluded_topic = util.excluded_topic_name(collection_num)

        for message_num in range(1, records_per_topic + 1):
            timestamp = int(time())

            logger.debug(f"Sending record {message_num}/{records_per_topic} to kafka topic {topic} "
                         f"& excluded topic '{excluded_topic}'.")

            producer.send(
                topic=topic,
                key=util.record_id(collection_num, message_num),
                value=util.body(message_num),
                timestamp_ms=timestamp
            )
            producer.send(
                topic=excluded_topic,
                key=util.record_id(collection_num, message_num),
                value=util.body(message_num),
                timestamp_ms=timestamp
            )

            logger.debug(f"Sent record {message_num}/{records_per_topic} to kafka topic '{topic}' "
                         f"& excluded topic '{excluded_topic}'.")

        logger.info(f"Published collection {collection_num}")

    logger.info("The kafka record publisher has finished.")


def main():
    publish_records()


if __name__ == '__main__':
    main()
