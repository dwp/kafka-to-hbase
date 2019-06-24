# pylint: disable=C

from assertpy import assert_that

from . import env


def test_delimited_value_conversion():
    def _assert(default, value, expected):
        converter = env.delimited(default)
        assert_that(converter(value)).is_equal_to(expected)

    for default, value, expected in [
        ("dflt", "one,two,three", ["one", "two", "three"]),
        ("dflt", "", ["dflt"]),
        ("dflt", None, ["dflt"]),
        ("", None, []),
    ]:
        yield _assert, default, value, expected


def test_string_value_conversion():
    def _assert(default, value, expected):
        converter = env.string(default)
        assert_that(converter(value)).is_equal_to(expected)

    for default, value, expected in [
        ("dflt", "testing", "testing"),
        ("dflt", "", "dflt"),
        ("dflt", None, "dflt"),
    ]:
        yield _assert, default, value, expected


def test_integer_value_conversion():
    def _assert(default, value, expected):
        converter = env.integer(default)
        assert_that(converter(value)).is_equal_to(expected)

    for default, value, expected in [
        (987, "1234", 1234),
        (987, "", 987),
        (987, None, 987),
    ]:
        yield _assert, default, value, expected


def test_boolean_value_conversion():
    def _assert(default, value, expected):
        converter = env.boolean(default)
        assert_that(converter(value)).is_equal_to(expected)

    for default, value, expected in [
        (False, "True", True),
        (False, "true", True),
        (False, "blah", False),
        (False, "", False),
        (False, None, False),
    ]:
        yield _assert, default, value, expected


def test_env_var_for_config_key_uppercases_and_prefixes():
    assert_that(env.env_var_for_config_key("kafka", "ssl_ciphers")
                ).is_equal_to("K2HB_KAFKA_SSL_CIPHERS")


def test_load_config_uses_defaults():
    assert_that(env.load_config({})).is_equal_to(env.Config(
        kafka=env.KafkaConfig(
            bootstrap_servers=["localhost:9092"],
            client_id="kafka-to-hbase",
            group_id="kafka-to-hbase",
            fetch_min_bytes=1,
            fetch_max_wait_ms=500,
            fetch_max_bytes=50 * 1024 * 1024,
            max_partition_fetch_bytes=1024 * 1024,
            request_timeout_ms=305000,
            retry_backoff_ms=100,
            reconnect_backoff_ms=50,
            reconnect_backoff_max_ms=1000,
            max_in_flight_requests_per_connection=5,
            check_crcs=True,
            metadata_max_age_ms=300000,
            session_timeout_ms=10000,
            heartbeat_interval_ms=3000,
            receive_buffer_bytes=32 * 1024,
            send_buffer_bytes=128 * 1024,
            consumer_timeout_ms=0,
            connections_max_idle_ms=540000,
            ssl=False,
            ssl_check_hostname=True,
            ssl_cafile="",
            ssl_certfile="",
            ssl_keyfile="",
            ssl_password="",
            ssl_crlfile="",
            ssl_ciphers="",
            sasl_kerberos=False,
            sasl_kerberos_service_name="kafka",
            sasl_kerberos_domain_name="",
            sasl_plain_username="",
            sasl_plain_password="",
            topics=[],
        ),
        hbase=env.HbaseConfig(
            host="localhost",
            port=9090,
            timeout=0,
            namespace="",
            table_prefix="",
            column="",
        ),
    ))


custom_environment = {
    "K2HB_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092,remotehost:9092",
    "K2HB_KAFKA_CLIENT_ID": "my-kafka-consumer",
    "K2HB_KAFKA_GROUP_ID": "group-id-123",
    "K2HB_KAFKA_FETCH_MIN_BYTES": "1024",
    "K2HB_KAFKA_FETCH_MAX_WAIT_MS": "1000",
    "K2HB_KAFKA_FETCH_MAX_BYTES": "65536",
    "K2HB_KAFKA_MAX_PARTITION_FETCH_BYTES": "32768",
    "K2HB_KAFKA_REQUEST_TIMEOUT_MS": "10000",
    "K2HB_KAFKA_RETRY_BACKOFF_MS": "50",
    "K2HB_KAFKA_RECONNECT_BACKOFF_MS": "2000",
    "K2HB_KAFKA_RECONNECT_BACKOFF_MAX_MS": "900",
    "K2HB_KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION": "10",
    "K2HB_KAFKA_CHECK_CRCS": "false",
    "K2HB_KAFKA_METADATA_MAX_AGE_MS": "3000",
    "K2HB_KAFKA_SESSION_TIMEOUT_MS": "1000",
    "K2HB_KAFKA_HEARTBEAT_INTERVAL_MS": "500",
    "K2HB_KAFKA_RECEIVE_BUFFER_BYTES": "8192",
    "K2HB_KAFKA_SEND_BUFFER_BYTES": "16384",
    "K2HB_KAFKA_CONSUMER_TIMEOUT_MS": "1000",
    "K2HB_KAFKA_CONNECTIONS_MAX_IDLE_MS": "3600000",
    "K2HB_KAFKA_SSL": "true",
    "K2HB_KAFKA_SSL_CHECK_HOSTNAME": "false",
    "K2HB_KAFKA_SSL_CAFILE": "/cafile",
    "K2HB_KAFKA_SSL_CERTFILE": "/cert",
    "K2HB_KAFKA_SSL_KEYFILE": "/key",
    "K2HB_KAFKA_SSL_PASSWORD": "password",
    "K2HB_KAFKA_SSL_CRLFILE": "/crl",
    "K2HB_KAFKA_SSL_CIPHERS": "sha256",
    "K2HB_KAFKA_SASL_KERBEROS": "true",
    "K2HB_KAFKA_SASL_KERBEROS_SERVICE_NAME": "test-kafka",
    "K2HB_KAFKA_SASL_KERBEROS_DOMAIN_NAME": "banana",
    "K2HB_KAFKA_SASL_PLAIN_USERNAME": "username",
    "K2HB_KAFKA_SASL_PLAIN_PASSWORD": "the_password",
    "K2HB_KAFKA_TOPICS": "hot,yellow",
    "K2HB_HBASE_HOST": "hbase1",
    "K2HB_HBASE_PORT": "9095",
    "K2HB_HBASE_TIMEOUT": "500",
    "K2HB_HBASE_NAMESPACE": "ns1",
    "K2HB_HBASE_TABLE_PREFIX": "tbl_",
    "K2HB_HBASE_COLUMN": "cf:data",
}


def test_load_config_uses_overrides():
    assert_that(env.load_config(custom_environment)).is_equal_to(env.Config(
        kafka=env.KafkaConfig(
            bootstrap_servers=["localhost:9092", "remotehost:9092"],
            client_id="my-kafka-consumer",
            group_id="group-id-123",
            fetch_min_bytes=1024,
            fetch_max_wait_ms=1000,
            fetch_max_bytes=65536,
            max_partition_fetch_bytes=32768,
            request_timeout_ms=10000,
            retry_backoff_ms=50,
            reconnect_backoff_ms=2000,
            reconnect_backoff_max_ms=900,
            max_in_flight_requests_per_connection=10,
            check_crcs=False,
            metadata_max_age_ms=3000,
            session_timeout_ms=1000,
            heartbeat_interval_ms=500,
            receive_buffer_bytes=8192,
            send_buffer_bytes=16384,
            consumer_timeout_ms=1000,
            connections_max_idle_ms=3600000,
            ssl=True,
            ssl_check_hostname=False,
            ssl_cafile="/cafile",
            ssl_certfile="/cert",
            ssl_keyfile="/key",
            ssl_password="password",
            ssl_crlfile="/crl",
            ssl_ciphers="sha256",
            sasl_kerberos=True,
            sasl_kerberos_service_name="test-kafka",
            sasl_kerberos_domain_name="banana",
            sasl_plain_username="username",
            sasl_plain_password="the_password",
            topics=["hot", "yellow"],
        ),
        hbase=env.HbaseConfig(
            host="hbase1",
            port=9095,
            timeout=500,
            namespace="ns1",
            table_prefix="tbl_",
            column="cf:data",
        ),
    ))


def test_env_vars_lists_all():
    assert_that(env.all_env_vars({})).is_equal_to({
        "K2HB_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "K2HB_KAFKA_CLIENT_ID": "kafka-to-hbase",
        "K2HB_KAFKA_GROUP_ID": "kafka-to-hbase",
        "K2HB_KAFKA_FETCH_MIN_BYTES": "1",
        "K2HB_KAFKA_FETCH_MAX_WAIT_MS": "500",
        "K2HB_KAFKA_FETCH_MAX_BYTES": "52428800",
        "K2HB_KAFKA_MAX_PARTITION_FETCH_BYTES": "1048576",
        "K2HB_KAFKA_REQUEST_TIMEOUT_MS": "305000",
        "K2HB_KAFKA_RETRY_BACKOFF_MS": "100",
        "K2HB_KAFKA_RECONNECT_BACKOFF_MS": "50",
        "K2HB_KAFKA_RECONNECT_BACKOFF_MAX_MS": "1000",
        "K2HB_KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION": "5",
        "K2HB_KAFKA_CHECK_CRCS": "true",
        "K2HB_KAFKA_METADATA_MAX_AGE_MS": "300000",
        "K2HB_KAFKA_SESSION_TIMEOUT_MS": "10000",
        "K2HB_KAFKA_HEARTBEAT_INTERVAL_MS": "3000",
        "K2HB_KAFKA_RECEIVE_BUFFER_BYTES": "32768",
        "K2HB_KAFKA_SEND_BUFFER_BYTES": "131072",
        "K2HB_KAFKA_CONSUMER_TIMEOUT_MS": "0",
        "K2HB_KAFKA_CONNECTIONS_MAX_IDLE_MS": "540000",
        "K2HB_KAFKA_SSL": "false",
        "K2HB_KAFKA_SSL_CHECK_HOSTNAME": "true",
        "K2HB_KAFKA_SSL_CAFILE": "",
        "K2HB_KAFKA_SSL_CERTFILE": "",
        "K2HB_KAFKA_SSL_KEYFILE": "",
        "K2HB_KAFKA_SSL_PASSWORD": "",
        "K2HB_KAFKA_SSL_CRLFILE": "",
        "K2HB_KAFKA_SSL_CIPHERS": "",
        "K2HB_KAFKA_SASL_KERBEROS": "false",
        "K2HB_KAFKA_SASL_KERBEROS_SERVICE_NAME": "kafka",
        "K2HB_KAFKA_SASL_KERBEROS_DOMAIN_NAME": "",
        "K2HB_KAFKA_SASL_PLAIN_USERNAME": "",
        "K2HB_KAFKA_SASL_PLAIN_PASSWORD": "",
        "K2HB_KAFKA_TOPICS": "",
        "K2HB_HBASE_HOST": "localhost",
        "K2HB_HBASE_PORT": "9090",
        "K2HB_HBASE_TIMEOUT": "0",
        "K2HB_HBASE_NAMESPACE": "",
        "K2HB_HBASE_TABLE_PREFIX": "",
        "K2HB_HBASE_COLUMN": ""
    })


def test_env_vars_lists_all_with_overrides():
    assert_that(env.all_env_vars(custom_environment)).is_equal_to({
        "K2HB_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092,remotehost:9092",
        "K2HB_KAFKA_CLIENT_ID": "my-kafka-consumer",
        "K2HB_KAFKA_GROUP_ID": "group-id-123",
        "K2HB_KAFKA_FETCH_MIN_BYTES": "1024",
        "K2HB_KAFKA_FETCH_MAX_WAIT_MS": "1000",
        "K2HB_KAFKA_FETCH_MAX_BYTES": "65536",
        "K2HB_KAFKA_MAX_PARTITION_FETCH_BYTES": "32768",
        "K2HB_KAFKA_REQUEST_TIMEOUT_MS": "10000",
        "K2HB_KAFKA_RETRY_BACKOFF_MS": "50",
        "K2HB_KAFKA_RECONNECT_BACKOFF_MS": "2000",
        "K2HB_KAFKA_RECONNECT_BACKOFF_MAX_MS": "900",
        "K2HB_KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION": "10",
        "K2HB_KAFKA_CHECK_CRCS": "false",
        "K2HB_KAFKA_METADATA_MAX_AGE_MS": "3000",
        "K2HB_KAFKA_SESSION_TIMEOUT_MS": "1000",
        "K2HB_KAFKA_HEARTBEAT_INTERVAL_MS": "500",
        "K2HB_KAFKA_RECEIVE_BUFFER_BYTES": "8192",
        "K2HB_KAFKA_SEND_BUFFER_BYTES": "16384",
        "K2HB_KAFKA_CONSUMER_TIMEOUT_MS": "1000",
        "K2HB_KAFKA_CONNECTIONS_MAX_IDLE_MS": "3600000",
        "K2HB_KAFKA_SSL": "true",
        "K2HB_KAFKA_SSL_CHECK_HOSTNAME": "false",
        "K2HB_KAFKA_SSL_CAFILE": "/cafile",
        "K2HB_KAFKA_SSL_CERTFILE": "/cert",
        "K2HB_KAFKA_SSL_KEYFILE": "/key",
        "K2HB_KAFKA_SSL_PASSWORD": "password",
        "K2HB_KAFKA_SSL_CRLFILE": "/crl",
        "K2HB_KAFKA_SSL_CIPHERS": "sha256",
        "K2HB_KAFKA_SASL_KERBEROS": "true",
        "K2HB_KAFKA_SASL_KERBEROS_SERVICE_NAME": "test-kafka",
        "K2HB_KAFKA_SASL_KERBEROS_DOMAIN_NAME": "banana",
        "K2HB_KAFKA_SASL_PLAIN_USERNAME": "username",
        "K2HB_KAFKA_SASL_PLAIN_PASSWORD": "the_password",
        "K2HB_KAFKA_TOPICS": "hot,yellow",
        "K2HB_HBASE_HOST": "hbase1",
        "K2HB_HBASE_PORT": "9095",
        "K2HB_HBASE_TIMEOUT": "500",
        "K2HB_HBASE_NAMESPACE": "ns1",
        "K2HB_HBASE_TABLE_PREFIX": "tbl_",
        "K2HB_HBASE_COLUMN": "cf:data",
    })
