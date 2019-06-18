""" Development tasks to simplify working with packages and docker """
from invoke import task
from invoke.exceptions import Exit


@task
def install(ctx):
    """ Install the package using dev dependencies and linked source """
    ctx.run("python3 -m pip install -e .[dev]")


@task
def build(ctx):
    """ Build the docker images used in development """
    ctx.run("docker-compose build")


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


@task(
    help={
        "topic": "a topic to listen to",
        "group_id": "the ID of the consumer group to join (default kafka-to-hbase)",
        "column": "the column name to store data in (defaults to cf:data)",
    },
    iterable=["topic"],
)
def run(ctx, topic, group_id="kafka-to-hbase", column="cf:data"):
    """ Run the kafka-to-hbase command locally with specified options """
    if not topic:
        raise Exit("No topics specified", 1)

    ctx.run(
        "python3 scripts/kafka-to-hbase",
        env={
            "K2HB_KAFKA_TOPICS": ",".join(topic),
            "K2HB_KAFKA_GROUP_ID": group_id,
            "K2HB_HBASE_COLUMN": column,
        }
    )


@task()
def run_docker(ctx, build_images=True, remove=True):
    """ Run the kafka-to-hbase command inside a properly configured docker container """
    if build_images:
        build(ctx)

    rm = '--rm' if remove else ''
    ctx.run(f"docker-compose run {rm} app kafka-to-hbase")
