import gzip
import json
from datetime import datetime, timedelta
from time import sleep

import boto3
import happybase

import util

from behave import given, then

logger = util.get_logger(__name__)


# HBase step implementations
@given(u'HBase is up and accepting connections')
def step_impl(context):
    connected = False

    while not connected:
        try:
            context.connection = happybase.Connection("hbase")
            context.connection.open()

            connected = True
        except Exception as e:
            print(e)
            print("Waiting for HBase connection...")
            sleep(2)
            continue


@then(u'HBase will have {num_of_tables} tables')
def step_impl(context, num_of_tables):
    num_of_tables = int(num_of_tables)
    expected_tables_sorted = [util.table_name(i) for i in range(1, num_of_tables + 1)]

    timeout_time = datetime.now() + timedelta(minutes=15)
    time_waited = 0

    # Checking for table count & names
    while timeout_time > datetime.now():
        tables = context.connection.tables()
        table_count = len(tables)

        logger.info(
            f"Waiting for {len(expected_tables_sorted)} hbase tables to appear; Found {len(tables)}; "
            f"Total of {time_waited} seconds elapsed"
        )

        if table_count == num_of_tables:
            break

        time_waited += 5
        sleep(5)
    else:  # Only executed when loop times out, break skips this
        raise AssertionError(f"Hbase only has {table_count} tables out of the expected {num_of_tables}")

    expected_tables_sorted.sort()
    tables.sort()

    assert table_count == num_of_tables
    assert tables == expected_tables_sorted, f"expected: {expected_tables_sorted}\nactual: {tables}"

    context.tables = tables


@then(u'each table will have {num_of_rows} rows')
def step_impl(context, num_of_rows):
    num_of_rows = int(num_of_rows)

    timeout_time = datetime.now() + timedelta(minutes=15)
    time_waited = 0

    tables_with_rows = []

    # Checking records within tables
    while timeout_time > datetime.now():

        if len(tables_with_rows) == len(context.tables):
            break

        # Looping over all table names
        for table_name in context.tables:
            assert "excluded" not in table_name.decode("utf-8"), f"Found 'excluded' in {table_name}"

            table = happybase.Table(table_name, context.connection)
            table_count = 0

            while table_count < num_of_rows:
                # .scan returns a generator so its easiest to convert to list to get the length
                table_count = len(list(table.scan()))

                logger.info(
                    f"Waiting for {num_of_rows} hbase records to appear in {table_name.decode('utf-8')}; "
                    f"Found {table_count}; "
                    f"Total of {time_waited} seconds elapsed"
                )

                if table_count == num_of_rows:
                    continue

                time_waited += 5
                sleep(5)
            else:
                tables_with_rows.append(table_name)


# S3 ucarchive step implementations
@given(u'all objects can be retrieved from the ucarchive S3 bucket')
def step_impl(context):
    s3 = boto3.resource("s3", endpoint_url="http://aws-s3:4566")
    bucket = s3.Bucket("ucarchive")

    context.s3_contents_list = []

    for i in bucket.objects.all():
        if i.key.endswith("jsonl.gz") and "load_test" in i.key:
            body = i.get().get("Body")
            body = gzip.decompress(body.read()).decode("utf-8")
            body = body.split("\n")

            for item in body:
                # Strips whitespace & checks if string is empty
                if not item.strip():
                    continue

                context.s3_contents_list.append(json.loads(item))

    assert len(context.s3_contents_list) > 1, "s3_contents_list is empty"


@then(u'the total size of the retrieved data should be {topic_count} topics with {records_per_topic} records')
def step_impl(context, topic_count, records_per_topic):
    expected = int(topic_count) * int(records_per_topic)
    actual = len(context.s3_contents_list)
    assert actual == expected, f"expected: {expected}, actual: {actual}"


@then(u'each of the objects should have the correct data')
def step_impl(context):
    for obj in context.s3_contents_list:
        try:
            trace_id = obj.get("traceId")
            message = obj.get("message")
            db_object = message.get("dbObject")

            assert trace_id == "00002222-abcd-4567-1234-1234567890ab", \
                f"expected: '00002222-abcd-4567-1234-1234567890ab', actual: '{trace_id}'"

            assert message is not None, f"expected message to not be None, message: {message}"

            assert db_object is not None and db_object == "bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fg" \
                                                          "h9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A", \
                f"expected db_object to not be None, message: {db_object}"
        except Exception as e:
            logger.error(f"failing object: {obj}")
            raise e


# S3 manifests step implementations
@given(u'all objects can be retrieved from the manifests S3 bucket')
def step_impl(context):
    s3 = boto3.resource("s3", endpoint_url="http://aws-s3:4566")
    bucket = s3.Bucket("manifests")

    context.s3_contents_list = []

    for i in bucket.objects.all():
        if i.key.endswith("txt") and "load-test" in i.key:
            body = i.get().get("Body").read()
            body = body.decode("utf-8")

            context.s3_contents_list.append(body)

    assert len(context.s3_contents_list) > 1, "s3_contents_list is empty"


@then(u'each of the objects should have the correct fields')
def step_impl(context):
    for obj in context.s3_contents_list:
        obj = obj.split("|")

        assert len(obj) == 8, f"expected object size of 8, actual: {len(obj)}"
        assert obj[0] != ""
        assert obj[0] is not None
        assert obj[1] != ""
        assert obj[1] is not None
        assert obj[2] != ""
        assert obj[2] is not None
        assert obj[3] != ""
        assert obj[3] is not None
        assert obj[4] == "STREAMED"
        assert obj[5] == "K2HB"
        assert obj[6] != ""
        assert obj[6] is not None
        assert obj[7] == "KAFKA_RECORD"

