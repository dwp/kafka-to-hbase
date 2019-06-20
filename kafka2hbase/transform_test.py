# pylint: disable=C

from assertpy import assert_that

from .transform import qualified_table_name


def test_adds_namespace_and_prefix():
    assert_that(qualified_table_name("ns", "pref", "topic")).is_equal_to("ns:pref_topic")


def test_omits_namespace_if_empty():
    assert_that(qualified_table_name("", "pref", "topic")).is_equal_to("pref_topic")


def test_omits_prefix_if_empty():
    assert_that(qualified_table_name("ns", "", "topic")).is_equal_to("ns:topic")


def test_returns_topic_if_no_ns_or_prefix():
    assert_that(qualified_table_name("", "", "topic")).is_equal_to("topic")
