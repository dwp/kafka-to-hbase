""" Simple test producer for checking kafka-to-hbase """

import argparse
import json
import random
from datetime import datetime
from uuid import uuid4

import pytz
from kafka import KafkaProducer


def timestamp_ms():
    """ Get the current time as milliseconds """
    ts = datetime.now(tz=pytz.utc)
    return ts.strftime(
        "%Y-%m-%dT%H:%M:%S.{ms}%z"
    ).format(ms=ts.microsecond // 1000)


def garbage(l):
    """ Generate random garbage of a given length """
    return "".join(random.choices(
        "abcdefghijklmnopqrstuvwxysABCDEFGHIJKLMNOPQRSTUVWXYZ", k=l))


def main():
    """ Entrypoint to the producer """
    parser = argparse.ArgumentParser("producer.py")
    parser.add_argument("topic", nargs='+', help="a topic to publish to")
    parser.add_argument("--count", "-c", type=int, default=1, help="the number of messages to publish")
    parser.add_argument("--max-length", "-l", type=int, default=1024, help="the maximum length of a message")
    args = parser.parse_args()

    producer = KafkaProducer()

    ix = 0
    done = False
    while not done:
        for topic in args.topic:
            # Generate the skeleton of a message
            message_id = str(uuid4())
            timestamp = timestamp_ms()
            m = {
                "id": message_id,
                "timestamp": timestamp,
                "topic": topic,
                "data": "",
            }

            # Fill the message with garbage to pad it
            current_length = len(json.dumps(m))
            if current_length < args.max_length:
                m["data"] = garbage(random.randint(1, args.max_length - current_length))

            # Calculate a key
            key = f"{topic}-{ix}"
            print(f"Sending {key} with id {message_id}")

            # Send as UTF-8 encoded JSON
            producer.send(
                topic,
                key=key.encode('utf8'),
                value=json.dumps(m).encode('utf8'),
                timestamp_ms=timestamp)

            ix += 1

            if ix >= args.count:
                done = True
                break

    producer.flush()


if __name__ == "__main__":
    main()
