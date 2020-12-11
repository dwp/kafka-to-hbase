import json
import logging
import time

from datetime import tzinfo
from datetime import datetime

db_name = "load-test-database"
collection_name = "load-test-collection"


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

    return logger


def topic_name(collection_number: int) -> str:
    return f"db.{db_name}{collection_number}.{collection_name}{collection_number}"


def excluded_topic_name(collection_number: int) -> str:
    return f"db.excluded.{collection_name}{collection_number}"


def record_id(collection_number: int, message_number: int) -> bytes:
    return f"key-{message_number}/{collection_number}".encode("utf-8")


def body(record_number: int) -> bytes:
    return json.dumps({
        "traceId": "00002222-abcd-4567-1234-1234567890ab",
        "unitOfWorkId": "00002222-abcd-4567-1234-1234567890ab",
        "@type": "V4",
        "version": "core-X.release_XXX.XX",
        "timestamp": "2018-12-14T15:01:02.000+0000",
        "message": {
            "@type": "MONGO_UPDATE",
            "collection": f"{collection_name}",
            "db": f"{db_name}",
            "_id": {
                "id": f"{db_name}/{collection_name}/{record_number}"
            },
            "_lastModifiedDateTime": f"{datetime.now().isoformat()[:-3]}+0000",
            "encryption": {
                "encryptionKeyId": "cloudhsm:1,2",
                "encryptedEncryptionKey": "bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab",
                "initialisationVector": "kjGyvY67jhJHVdo2",
                "keyEncryptionKeyId": "cloudhsm:1,2"
            },
            "dbObject": "bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A",
            "timestamp_created_from": "_lastModifiedDateTime"
        }
    }).encode("utf-8")
