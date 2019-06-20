""" Test invocation tasks """

from invoke import task


@task
def unit(ctx):
    """ Run all unit tests """
    ctx.run("python3 -m nose")


@task
def integration(ctx):
    """ Run all integration tests """
    ctx.run("docker-compose run --rm integration-tests behave")
