import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.logging.Logger
import com.beust.klaxon.JsonObject
import org.bson.BsonDocument

fun shovelAsync(kafka: KafkaConsumer<ByteArray, ByteArray>, hbase: HbaseClient, pollTimeout: Duration) =
    GlobalScope.async {
        val log = Logger.getLogger("shovelAsync")

        log.info(Config.Kafka.reportTopicSubscriptionDetails())

        while (isActive) {
            kafka.subscribe(Config.Kafka.topicRegex)
            val records = kafka.poll(pollTimeout)
            for (record in records) {
                
                val key = generateKey(record.value())

                if (key.isEmpty()) {
                    log.warning(
                        "Empty key was skipped for %s:%d:%d".format(
                            record.topic() ?: "null",
                            record.partition(),
                            record.offset()
                        ))
                    continue
                }

                try {
                    hbase.putVersion(
                        topic = record.topic().toByteArray(),
                        key = record.key(),
                        body = record.value(),
                        version = record.timestamp()
                    )
                    log.info(
                        "Wrote key %s data %s:%d:%d".format(
                            String(key),
                            record.topic() ?: "null",
                            record.partition(),
                            record.offset()
                        )
                    )
                } catch (e: Exception) {
                    log.severe(
                        "Error while writing key %s data %s:%d:%: %s".format(
                            String(key),
                            record.topic() ?: "null",
                            record.partition(),
                            record.offset(),
                            e.toString()
                        )
                    )
                    throw e
                }
            }
        }
    }

fun generateKey(body: ByteArray): ByteArray {
    val log = Logger.getLogger("generateKey")

    try {
        val json: JsonObject = convertToJson(body)
        val bson: BsonDocument = convertToBson(json)
        val hash: String = generateHash("sha-1", json.toJsonString())
        val key_string: String = "%s%s".format(hash, bson.toString())
        return encodeToBase64(key_string).toByteArray()
    } catch (e: IllegalArgumentException) {
        log.warning("Could not parse message body, so will skip record")
        return ByteArray(0)
    }
}