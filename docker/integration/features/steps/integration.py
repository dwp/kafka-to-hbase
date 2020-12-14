import gzip
import json
from datetime import datetime, timedelta
from time import sleep

import boto3
import happybase

import util

from behave import given, then

logger = util.get_logger(__name__)

s3_bucket = "ucarchive"
s3_dir = "ucdata_main"


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
                # .scan returns a generator so its easiest to convert to list for len
                table_count = len(list(table.scan()))

                logger.info(
                    f"Waiting for {num_of_rows} hbase records to appear in {table_name.decode('utf-8')}; "
                    f"Found {table_count}; "
                    f"Total of {time_waited} seconds elapsed"
                )

                if table_count == 1000:
                    continue

                time_waited += 5
                sleep(5)
            else:
                tables_with_rows.append(table_name)


# S3 step implementations
@given(u'all objects can be retrieved from S3')
def step_impl(context):
    s3 = boto3.resource("s3", endpoint_url="http://aws-s3:4566")
    bucket = s3.Bucket(s3_bucket)

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


@then(u'the total size of the retrieved data should be {topic_count} * {records_per_topic}')
def step_impl(context, topic_count, records_per_topic):
    # logger.info(context.s3_contents_list)
    expected = int(topic_count) * int(records_per_topic)
    actual = len(context.s3_contents_list)
    assert actual == expected, f"expected: {expected}, actual: {actual}"


@then(u'each of the objects should have the correct data')
def step_impl(context):
    raise NotImplementedError(u'STEP: Then each of the objects should have the correct data')
