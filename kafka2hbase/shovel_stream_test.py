# pylint: disable=C

from unittest import mock

from assertpy import assert_that

from .message import Message
from .shovel_stream import shovel


def test_stores_valid_messages():
    store = mock.MagicMock()
    messages = [
        Message("test-topic", "test-key", "test-value", 1645275945),
        Message("another-topic", "another-key", "another-value", 185735485),
    ]

    count = shovel(messages, store, lambda x: x)

    assert_that(count).is_equal_to(2)
    assert_that(store.call_count).is_equal_to(2)
    store.assert_any_call("test-topic", "test-key", "test-value", 1645275945)
    store.assert_any_call("another-topic", "another-key", "another-value", 185735485)


def test_uses_destination_function():
    store = mock.MagicMock()
    messages = [
        Message("test-topic", "test-key", "test-value", 1645275945)
    ]

    count = shovel(messages, store, lambda x: x + "-transformed")

    assert_that(count).is_equal_to(1)
    store.assert_called_once_with("test-topic-transformed", "test-key", "test-value", 1645275945)


def test_ignores_invalid_message():
    def _test(message):
        store = mock.MagicMock()
        messages = [message]
        count = shovel(messages, store, lambda x: x)

        assert_that(count).is_equal_to(1)
        store.assert_not_called()

    messages = [
        Message("", "test-key", "test-value", 1645275945),
        Message("test-topic", "", "test-value", 1645275945),
        Message("test-topic", "test-key", "", 1645275945),
        Message("test-topic", "test-key", "test-value", 0),
    ]

    for message in messages:
        yield _test, message
