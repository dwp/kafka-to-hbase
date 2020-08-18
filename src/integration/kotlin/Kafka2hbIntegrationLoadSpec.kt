
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import lib.getISO8601Timestamp
import lib.sendRecord
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.Logger
import kotlin.time.ExperimentalTime
import kotlin.time.minutes
import kotlin.time.seconds

@ExperimentalTime
class Kafka2hbIntegrationLoadSpec : StringSpec() {

    companion object {
        private val log = Logger.getLogger(Kafka2hbIntegrationLoadSpec::class.toString())
        private const val TOPIC_COUNT = 10
        private const val RECORDS_PER_TOPIC = 10_000
        private const val DB_NAME = "load-test-database"
        private const val COLLECTION_NAME = "load-test-collection"
    }

    init {
        "Many messages sent to many topics" {
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            val converter = Converter()
            repeat(TOPIC_COUNT) { collectionNumber ->
                val topic = topicName(collectionNumber)
                repeat(RECORDS_PER_TOPIC) { messageNumber ->
                    launch {
                        val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
                        log.info("Sending record $messageNumber/$RECORDS_PER_TOPIC to kafka topic '$topic'.")
                        producer.sendRecord(topic.toByteArray(), recordId(collectionNumber, messageNumber), body(messageNumber), timestamp)
                    }
                }
            }

            HbaseClient.connect().use { hbase ->
                withTimeout(5.minutes) {
                    while (expectedTables != loadTestTables(hbase)) {
                        println("Waiting for tables to appear")
                        delay(2.seconds)
                    }

                    loadTestTables(hbase).forEach { tableName ->
                        hbase.connection.getTable(TableName.valueOf(tableName)).use { table ->
                            while (recordCount(table) != RECORDS_PER_TOPIC) {
                                println("Waiting for records to appear")
                                delay(2.seconds)
                            }
                        }
                    }
                }
            }
        }
    }

    private fun recordCount(table: Table) = table.getScanner(Scan()).count()


    private val expectedTables by lazy {
        (0..9).map { tableName(it) }
    }

    private fun loadTestTables(hbase: HbaseClient)
            = hbase.connection.admin.listTableNames()
            .map { it.nameAsString }
            .filter { Regex(tableNamePattern()).matches(it) }

    private fun tableName(it: Int) = "$DB_NAME$it:$COLLECTION_NAME$it".replace("-", "_")

    private fun tableNamePattern() = """$DB_NAME\d+:$COLLECTION_NAME\d+""".replace("-", "_")

    private fun topicName(collectionNumber: Int)
            = "db.$DB_NAME$collectionNumber.$COLLECTION_NAME$collectionNumber"

    private fun recordId(collectionNumber: Int, messageNumber: Int) =
            "key-$messageNumber/$collectionNumber".toByteArray()

    private fun body(recordNumber: Int) = """{
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


