""" Test invocation tasks """

from invoke import task


@task
def unit(c):
    """ Run all unit tests """
    c.run("python3 -m nose")
