import happybase
import time

from datetime import datetime


def for_fn(fn, timeout=10):
    start = datetime.now().timestamp()
    value = None

    while datetime.now().timestamp() - start < timeout:
        value = fn()
        if value:
            return value
        time.sleep(0.1)

    raise TimeoutError(
        f"Failed waiting for {fn.__name__} after {timeout} seconds"
    )


def for_hbase_cell_version_after(timestamp, hbase: happybase.Connection, table, key, column, versions=10, timeout=10):
    def _get_row():
        row = hbase.table(table).row(key, include_timestamp=True)
        cell = row.get(column, (b'', 0))

        if cell[1] > timestamp:
            return cell

        return None

    return for_fn(_get_row, timeout)
