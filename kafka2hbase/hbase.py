""" Hbase functions for storing messages """

import logging


_log = logging.getLogger(__name__)


def hbase_put(hbase, table, key, value, timestamp):
    """ Put a single value into Hbase """
    _log.debug("Writing %s to %s at %s version %s", value, table, key, timestamp)
    _log.info("Writing %s to %s version %s", key, value, timestamp)
    hbase.table(table).put(key, value, timestamp)
