""" Transform functions """


def qualified_table_name(namespace, prefix, topic):
    """ Calculate the fully qualified table name for a topic including namespace and prefix """
    fqtn = ""

    if namespace:
        fqtn += namespace + ":"

    if prefix:
        fqtn += prefix + "_"

    return fqtn + topic
