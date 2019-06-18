""" Development tasks to simplify working with packages and docker """
from invoke import task


@task
def install(ctx):
    """ Install the package using dev dependencies and linked source """
    ctx.run("python3 -m pip install -e .[dev] --force-reinstall")


@task
def build(ctx):
    """ Build the docker images used in development """
    ctx.run("docker-compose build")


@task()
def run(ctx, build_images=True, remove=True):
    """ Run the kafka-to-hbase command inside a properly configured docker container """
    if build_images:
        build(ctx)

    rm = '--rm' if remove else ''
    ctx.run(f"docker-compose run {rm} app kafka-to-hbase")


@task()
def up(ctx, build_images=True):
    """ Bring up the development stack including Kafka and Hbase """
    if build_images:
        build(ctx)

    ctx.run("docker-compose up -d")


@task
def down(ctx):
    """ Bring down the development environment """
    ctx.run("docker-compose down")


@task
def purge(ctx):
    """ Remove old containers and purge all data """
    ctx.run("docker-compose rm -fv")
    ctx.run("docker volume prune -f")
    ctx.run("docker network prune -f")


@task
def cleanup(ctx):
    """ Reset all changes and delete all untracked files in the repository """
    ctx.run("git clean -fdx")
