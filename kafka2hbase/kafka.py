""" Stream data from Kafka topics """

import logging

from time import sleep
from datetime import datetime

from .message import Message


_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)


def timestamp():
    """ Get the current unix timestamp in seconds as a float """
    return datetime.now().timestamp()


def sleep_ms(ms):
    """ Sleep for a number of milliseconds """
    sleep(ms / 1000)


def kafka_stream(consumer, timeout=5):
    """ Stream data from kafka until there is no more or the timeout expires """
    # Wait timeout_ms to see if any messages come in if there are none immediately
    start = timestamp()
    topic_messages = None
    while not topic_messages and timestamp() - start < timeout:
        _log.debug("Waiting for messages from %s", consumer)
        sleep_ms(10)
        topic_messages = consumer.poll()

    # If none arrived then give up
    if not topic_messages:
        _log.info("Exceeded %ds waiting for messages", timeout)
        return

    # Generate a sequence of all messages from all topics
    _log.debug("Received topic messages for %d topics", len(topic_messages))
    messages = (message for messages in topic_messages.values() for message in messages)
    for message in messages:
        if not message.topic:
            _log.warning("message %s missing topic", message)
            continue

        if not message.key:
            _log.warning("message %s missing key", message)
            continue

        if not message.value:
            _log.warning("message %s missing value", message)
            continue

        # if not message.timestamp:
        #     _log.warning("message %s missing timestamp", message)
        #     continue

        wrapped = Message(
            message.topic,
            message.key,
            message.value,
            int(timestamp() * 1000))
        _log.debug("Yielding message %s", wrapped)
        yield wrapped

    _log.debug("Committing offsets")
    consumer.commit()
