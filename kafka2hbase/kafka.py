""" Stream data from Kafka topics """

import logging

from time import sleep
from datetime import datetime

from .message import Message


_log = logging.getLogger(__name__)


def timestamp():
    """ Get the current unix timestamp in seconds as a float """
    return datetime.now().timestamp()


def kafka_stream(consumer, timeout=10):
    """ Stream data from kafka until there is no more or the timeout expires """
    # Loop continuously until we get no messages during a timeout window
    start = timestamp()
    topic_messages = {}
    while timestamp() - start < timeout:
        # Try and get some messages, retrying if required
        _log.debug("Waiting for messages from %s", consumer)
        topic_messages = consumer.poll()
        if topic_messages:
            break

        sleep(1)

    # If the timeout has been reached then there were never any to start with
    if not topic_messages:
        _log.info("Exceeded %ds waiting for messages", timeout)

    # Generate a sequence of all messages from all topics
    _log.info("Received %s", {x.topic: len(topic_messages[x]) for x in topic_messages})
    yield from (
        Message(
            message.topic,
            message.partition,
            message.offset,
            message.key,
            message.value,
            int(timestamp() * 1000)  # TODO Use timestamp from message
        )
        for messages in topic_messages.values()
        for message in messages
    )

    _log.debug("Committing offsets")
    consumer.commit()
