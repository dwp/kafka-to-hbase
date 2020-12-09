from time import sleep

import happybase
import requests

from util import table_name

from behave import given, then


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
    expected_tables_sorted = [table_name(i) for i in range(0, num_of_tables)]

    print(expected_tables_sorted)
