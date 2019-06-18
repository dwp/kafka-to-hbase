""" Core functionality for Kafka2Hbase """

import logging


_log = logging.getLogger(__name__)


def qualified_table_name(namespace, prefix, name):
    """ Calculate the fully qualified table name for a topic including namespace and prefix """
    fqtn = ""

    if namespace:
        fqtn += namespace + ":"

    if prefix:
        fqtn += prefix + "_"

    return fqtn + name


def has_truthy_attr(obj, attr):
    """ Check if an object attribute exists and has a truthy value """
    if not getattr(obj, attr, None):
        return False
    return True


def shovel(stream, store, get_destination):
    """ Shovel data from a stream into a data store until there are none left """
    messages_processed = 0

    for message in stream:
        messages_processed += 1

        # Check that all attributes are set
        valid = True
        for attr in ["topic", "key", "value", "timestamp"]:
            if not has_truthy_attr(message, attr):
                _log.warning("Message %s missing %s", message, attr)
                valid = False
                break

        # If anything is wrong just don't process
        if not valid:
            continue

        # Get the table name and store the message
        table_name = get_destination(message.topic)
        store(table_name, message.key, message.value, message.timestamp)

    return messages_processed
