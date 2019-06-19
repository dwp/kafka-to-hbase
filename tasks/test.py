""" Test invocation tasks """

from invoke import task
from invoke.exceptions import Exit


@task
def unit(ctx):
    """ Run all unit tests """
    ctx.run("python3 -m nose")
