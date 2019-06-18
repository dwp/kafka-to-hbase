""" Test invocation tasks """

from invoke import task
from invoke.exceptions import Exit


@task
def unit(ctx):
    """ Run all unit tests """
    ctx.run("python3 -m nose")


@task(
    help={
        "topic": "a topic to produce messages to",
        "count": "the number of messages to produce (default 1)",
    },
    iterable=["topic"],
)
def produce(ctx, topic, count=1):
    """ Produce test messages on specified topics """
    if not topic:
        raise Exit("No topics specified", 1)

    ctx.run("python3 utilities/producer.py " + " ".join(topic) + f" --count {count}")
