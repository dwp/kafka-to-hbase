""" A wrapper for the Kafka message containing only required fields """


from collections import namedtuple


class Message(namedtuple("Message", ["topic", "key", "value", "timestamp"])):
    """ A simplified version of the Kafka message containing only required fields """
