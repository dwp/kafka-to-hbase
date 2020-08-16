
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import lib.getISO8601Timestamp
import lib.sendRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.Logger

class Kafka2hbIntegrationLoadSpec : StringSpec() {

    companion object {
        private val log = Logger.getLogger(Kafka2hbIntegrationLoadSpec::class.toString())
        private const val COLLECTION_COUNT = 10
        private const val RECORDS_PER_COLLECTION = 10
        private const val DB_NAME = "load-test-database"
        private const val COLLECTION_NAME = "load-test-collection"
    }

    init {
        "Send many messages to many collections for a load test" {
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            val converter = Converter()
            repeat(COLLECTION_COUNT) { collectionNumber ->
                val topic = topicName(collectionNumber)
                repeat(RECORDS_PER_COLLECTION) { messageNumber ->
                    launch {
                        val body = ucRecord(messageNumber)
                        val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
                        log.info("Sending record $messageNumber/$RECORDS_PER_COLLECTION to kafka topic '$topic'.")
                        producer.sendRecord(topic.toByteArray(), recordId(collectionNumber, messageNumber), body, timestamp)
                    }
                }
            }

            withTimeout(5 * 60 * 1_000) {
                HbaseClient.connect().use { hbase ->
                    var allPresent = false
                    while (!allPresent) {
                        val loadTestTables = hbase.connection.admin.listTableNames()
                                .map { it.nameAsString }
                                .filter { Regex(tableNamePattern()).matches(it) }
                        val expected = (0..9).map { tableName(it) }
                        allPresent = loadTestTables == expected
                    }
                }
            }
        }
    }

    private fun tableName(it: Int) = "$DB_NAME$it:$COLLECTION_NAME$it".replace("-", "_")

    private fun tableNamePattern() = """$DB_NAME\d+:$COLLECTION_NAME\d+""".replace("-", "_")

    private fun topicName(collectionNumber: Int)
            = "db.$DB_NAME$collectionNumber.$COLLECTION_NAME$collectionNumber"

    private fun recordId(collectionNumber: Int, messageNumber: Int) =
            "key-$messageNumber/$collectionNumber".toByteArray()

    private fun ucRecord(recordNumber: Int) = """{
        "traceId": "00002222-abcd-4567-1234-1234567890ab",
        "unitOfWorkId": "00002222-abcd-4567-1234-1234567890ab",
        "@type": "V4",
        "version": "core-X.release_XXX.XX",
        "timestamp": "2018-12-14T15:01:02.000+0000",
        "message": {
            "@type": "MONGO_UPDATE",
            "collection": "$COLLECTION_NAME",
            "db": "$DB_NAME",
            "_id": {
                "id": "$DB_NAME/$COLLECTION_NAME/$recordNumber"
            },
            "_lastModifiedDateTime": "${getISO8601Timestamp()}",
            "encryption": {
                "encryptionKeyId": "cloudhsm:1,2",
                "encryptedEncryptionKey": "bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab",
                "initialisationVector": "kjGyvY67jhJHVdo2",
                "keyEncryptionKeyId": "cloudhsm:1,2"
            },
            "dbObject": "bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A",
            "timestamp_created_from": "_lastModifiedDateTime"
        }
    }""".toByteArray()

}
