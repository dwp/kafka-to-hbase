import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import lib.*
import org.apache.kafka.clients.producer.KafkaProducer

class Kafka2Hbase : StringSpec({
    configureLogging()

    val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.props)
    val hbase = HbaseClient.connect()

    "messages with new identifiers are written to hbase" {
        val topic = uniqueTopicName()
        val startingCounter = waitFor { hbase.getCount(topic) }

        val body = uniqueBytes()
        val timestamp = getTimestampAsLong(getISO8601Timestamp())
        val key = uniqueBytes()
        producer.sendRecord(topic, key, body, timestamp)

        val storedValue = waitFor { hbase.getCellInTimestampsRange(topic, key,0,timestamp) }
        storedValue shouldBe body

        val counter = waitFor { hbase.getCount(topic) }
        counter shouldBe startingCounter + 1
    }

    "messages with previously received identifiers are written as new versions" {
        val topic = uniqueTopicName()
        val startingCounter = waitFor { hbase.getCount(topic) }

        val body = uniqueBytes()
        val timestamp = getTimestampAsLong(getISO8601Timestamp())
        val key = uniqueBytes()
        hbase.putVersion(topic, key, body, timestamp)

        val newBody = uniqueBytes()
        val newTimestamp = getTimestampAsLong(getISO8601Timestamp())
        producer.sendRecord(topic, key, newBody, newTimestamp)

        val storedNewValue = waitFor { hbase.getCellInTimestampsRange(topic, key, timestamp, newTimestamp) }
        storedNewValue shouldBe newBody

        val storedPreviousValue = waitFor { hbase.getCellInTimestampsRange(topic, key, 0,timestamp) }
        storedPreviousValue shouldBe body

        val counter = waitFor { hbase.getCount(topic) }
        counter shouldBe startingCounter + 2
    }

    "messages with empty key are skipped" {
        val topic = uniqueTopicName()
        val startingCounter = waitFor { hbase.getCount(topic) }

        val body = uniqueBytes()
        val timestamp = timestamp()
        producer.sendRecord(topic, ByteArray(0), body, timestamp)

        val counter = waitFor { hbase.getCount(topic) }
        counter shouldBe startingCounter
    }

})