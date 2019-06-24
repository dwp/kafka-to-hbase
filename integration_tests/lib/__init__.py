from datetime import datetime


def timestamp_ms():
    return (datetime.now().timestamp() * 1000)
