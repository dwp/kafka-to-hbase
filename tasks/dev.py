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

    from .hbase import create_namespace, create_table
    create_namespace(ctx, "k2hb")
    create_table(ctx, ["k2hb:docker"])
    create_table(ctx, ["k2hb:integration-test"])


@task
def down(ctx):
    """ Bring down the development environment """
    ctx.run("docker-compose down")


@task
def purge(ctx):
    """ Remove old containers and purge all data """
    ctx.run("docker-compose rm -sfv")
    ctx.run("docker volume prune -f")
    ctx.run("docker network prune -f")


@task
def cleanup(ctx):
    """ Reset all changes and delete all untracked files in the repository """
    ctx.run("git clean -fdx")


@task(
    help={
        "topic": "a topic to listen to",
        "group-id": "the ID of the consumer group to join (default kafka-to-hbase)",
        "column": "the column name to store data in (defaults to cf:data)",
    },
    iterable=["topic"],
)
def run(ctx, topic, group_id="kafka-to-hbase", column="cf:data"):
    """ Run the kafka-to-hbase command locally with specified options """
    from shlex import quote

    if not topic:
        raise Exit("No topics specified", 1)

    topics = ",".join(topic)
    ctx.run(
        f"docker-compose run --rm -e K2HB_KAFKA_TOPICS={quote(topics)} -e K2HB_KAFKA_GROUP_ID={quote(group_id)} -e K2HB_HBASE_COLUMN={quote(column)} python scripts/kafka-to-hbase",
    )


@task(
    help={
        "topic": "a topic to produce messages to",
        "count": "the number of messages to produce (default 1)",
    },
    iterable=["topic"],
)
def producer(ctx, topic, count=1):
    """ Produce test messages on specified topics """
    if not topic:
        raise Exit("No topics specified", 1)

    topics = " ".join(topic)
    ctx.run(
        f"docker-compose run --rm python utilities/producer.py {topics} --count {count}"
    )
