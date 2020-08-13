import com.nhaarman.mockitokotlin2.*
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.io.IOException


class ListProcessorTest : StringSpec() {

    init {

        "Only commits offsets on success, resets position on failure" {
            val validator = mock<Validator>()
            val recordProcessor = ListProcessor(validator, Converter())
            val hbaseClient = mock<HbaseClient> {
                on { putList(any(), any()) } doAnswer {
                    val tableName = it.getArgument<String>(0)
                    val matchResult = Regex("""[13579]$""").find(tableName)
                    if (matchResult != null) {
                        throw IOException("Table: '$tableName'.")
                    }
                }
            }

            val consumer = mock<KafkaConsumer<ByteArray, ByteArray>> {
                (1..10).forEach { topicNumber ->
                    on {
                        committed(TopicPartition("db.database$topicNumber.collection$topicNumber", 10 - topicNumber))
                    } doReturn OffsetAndMetadata((topicNumber * 10).toLong(), "")
                }
            }

            val parser = mock<MessageParser> {
                val hbaseKeys = (1..1000000).map { Bytes.toBytes(it) }
                on { generateKeyFromRecordBody(any()) } doReturnConsecutively hbaseKeys
            }

            val map = (1..10).associate { topicNumber ->
                TopicPartition("db.database$topicNumber.collection$topicNumber", 10 - topicNumber) to (1..100).map { recordNumber ->
                    val body = Bytes.toBytes(json(recordNumber))
                    val key = Bytes.toBytes(recordNumber)
                    mock<ConsumerRecord<ByteArray, ByteArray>> {
                        on { value() } doReturn body
                        on { key() } doReturn key
                        on { offset() } doReturn (topicNumber * recordNumber * 20).toLong()
                    }
                }
            }

            recordProcessor.processRecords(hbaseClient, consumer, parser, ConsumerRecords<ByteArray, ByteArray>(map))

            val tableNameCaptor = argumentCaptor<String>()
            val recordCaptor = argumentCaptor<List<HbasePayload>>()
            verify(hbaseClient, times(10)).putList(tableNameCaptor.capture(), recordCaptor.capture())
            tableNameCaptor.allValues shouldBe (1..10).map { "database$it:collection$it" }

            val commitCaptor = argumentCaptor<Map<TopicPartition, OffsetAndMetadata>>()
            verify(consumer, times(5)).commitSync(commitCaptor.capture())
            commitCaptor.allValues.forEachIndexed { index, element ->
                val topicNumber = (index + 1) * 2
                element.size shouldBe 1
                val topicPartition = TopicPartition("db.database$topicNumber.collection$topicNumber", 10 - topicNumber)
                element[topicPartition] shouldNotBe null
                element[topicPartition]?.offset() shouldBe (topicNumber * 20 * 100) + 1
            }

            val topicPartitionCaptor = argumentCaptor<TopicPartition>()
            val committedCaptor = argumentCaptor<TopicPartition>()
            val positionCaptor = argumentCaptor<Long>()
            verify(consumer, times(5)).committed(committedCaptor.capture())

            committedCaptor.allValues.forEachIndexed { index, topicPartition ->
                val topic = topicPartition.topic()
                val partition = topicPartition.partition()
                val topicNumber = (index * 2 + 1)
                partition shouldBe 10 - topicNumber
                topic shouldBe "db.database$topicNumber.collection$topicNumber"
            }

            verify(consumer, times(5)).seek(topicPartitionCaptor.capture(), positionCaptor.capture())

            topicPartitionCaptor.allValues.zip(positionCaptor.allValues).forEachIndexed { index, pair ->
                println("INDEX: $index")
                val topicNumber = index * 2 + 1
                val topicPartition = pair.first
                val position = pair.second
                val topic = topicPartition.topic()
                val partition = topicPartition.partition()
                topic shouldBe "db.database$topicNumber.collection$topicNumber"
                partition shouldBe 10 - topicNumber
                position shouldBe topicNumber * 10
            }

            verifyNoMoreInteractions(consumer)
        }
    }

    private fun json(id: Any) =
        """
        {
            "message": {
                "_id": {
                    "id": "$id" 
                }
            }
        }
        """.trimIndent()

}
