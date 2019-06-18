""" Hbase management utilities """

import happybase
import subprocess

from invoke import task
from invoke.exceptions import Exit


def connect():
    """ Connect to Hbase on localhost """
    return happybase.Connection()


@task(
    help={
        "name": "fully qualified table name",
        "family": "the name of the column family (default cf)",
        "versions": "the maximum number of versions to support (default 10)"
    },
    iterable=['name'],
)
def create_table(ctx, name, family="cf", versions=10):  # pylint: disable=unused-argument
    """ Create a new table """
    if not name:
        raise Exit("No table names specified", 1)

    hbase = connect()
    for table in name:
        hbase.create_table(
            table,
            {
                family: {
                    "max_versions": versions,
                }
            }
        )


@task(
    help={
        "name": "fully qualified table name",
    },
    iterable=['name'],
)
def drop_table(ctx, name):  # pylint: disable=unused-argument
    """ Drop a table """
    if not name:
        raise Exit("No table names specified", 1)

    hbase = connect()
    for table in name:
        hbase.delete_table(table, disable=True)


@task
def show_tables(ctx):  # pylint: disable=unused-argument
    """ Create a new table """
    hbase = connect()
    for table in hbase.tables():
        print(table.decode('utf8'))


@task
def shell(ctx):
    """ Open an Hbase shell """
    ctx.run("docker-compose exec hbase hbase shell", pty=True)
