""" Stream data from Kafka topics """

import logging

from time import sleep
from datetime import datetime

from .message import Message


_log = logging.getLogger(__name__)


def timestamp():
    """ Get the current unix timestamp in seconds as a float """
    return datetime.now().timestamp()


def kafka_stream(consumer):
    """ Stream all ready data from kafka """
    topic_messages = consumer.poll()
    if not topic_messages:
        return

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
