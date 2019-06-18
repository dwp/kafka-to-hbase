""" Simple test producer for checking kafka-to-hbase """

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


def encryption(master_key_id, key_id, encrypted_key, init_vector):
    """ Generate a nonsense encryption block """
    return {
        "encryptionKeyId": key_id,
        "encryptedEncryptionKey": encrypted_key,
        "initialisationVector": init_vector,
        "keyEncryptionKeyId": master_key_id,
    }


def generate_message(collection, db, id_key, id_value, timestamp, enc_keys, data):  # pylint: disable=too-many-arguments
    """ Generate a random, well formed UCFS message """
    return {
        "@type": "MONGO_UPDATE",
        "collection": collection,
        "db": db,
        "_id": {
            id_key: id_value,
        },
        "_lastModifiedDateTime": timestamp,
        "encryption": enc_keys,
        "dbObject": data,
    }


def wrap_message(message):
    """ Wrap the message in a UCFS envelope """
    return {
        "traceId": str(uuid4()),
        "unitOfWorkId": str(uuid4()),
        "@type": "V4",
        "version": "core-X.release_XXX.XX",
        "timestamp": "2018-12-14T15:01:02.000+0000",
        "message": message,
    }


def main():
    """ Entrypoint to the producer """
    producer = KafkaProducer()
    topics = set()

    for db in ['claimant', 'payment', 'containment', 'raiment']:
        for coll in ['details', 'retails', 'emails', 'seasnails']:
            ik = coll[0:-1] + "Id"
            for x in range(0, 100):
                enc_keys = encryption(
                    str(uuid4()),
                    str(uuid4())[28:],
                    garbage(32),
                    garbage(16),
                )
                m = wrap_message(
                    generate_message(
                        coll,
                        db,
                        ik,
                        str(uuid4()),
                        timestamp_ms(),
                        enc_keys,
                        garbage(1024),
                    )
                )

                topic = f"{db}.{coll}"
                topics.add(topic)

                producer.send(
                    topic,
                    key=f"{x}".encode('utf8'),
                    value=json.dumps(m).encode('utf8'),
                    timestamp_ms=timestamp_ms())

    producer.flush()

    print(",".join(topics))


if __name__ == "__main__":
    main()
