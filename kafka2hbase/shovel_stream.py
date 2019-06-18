""" Core functionality for Kafka2Hbase """


def qualified_table_name(namespace, prefix, name):
    """ Calculate the fully qualified table name for a topic including namespace and prefix """
    fqtn = ""

    if namespace:
        fqtn += namespace + ":"

    if prefix:
        fqtn += prefix + "_"

    return fqtn + name


def shovel(stream, store, get_destination):
    """ Shovel data from a stream into a data store until there are none left """
    messages_processed = 0

    for message in stream:
        messages_processed += 1
        table_name = get_destination(message.topic)
        store(table_name, message.key, message.value, message.timestamp)

    return messages_processed
