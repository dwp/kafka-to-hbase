from assertpy import assert_that

from lib import clients, wait, timestamp_ms


def test_valid_message_writes_new_hbase_row():
    timestamp = timestamp_ms()

    producer = clients.create_kafka_producer()
    producer.send(
        topic="integration-test",
        key=b"woo",
        value=b"THE CONTENT OF THE THING",
        timestamp_ms=timestamp)
    producer.flush()

    hbase = clients.create_hbase_client()
    cell = wait.for_hbase_cell_version_after(
        timestamp, hbase, "k2hb:integration-test", b"woo", b"cf:data")

    assert_that(cell[0]).is_equal_to(b"THE CONTENT OF THE THING")
    assert_that(cell[1]).is_greater_than(timestamp)
