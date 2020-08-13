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


class ListProcessorTest : StringSpec() {

    init {

        "works" {
            val validator = mock<Validator>()
            val recordProcessor = ListProcessor(validator, Converter())
            val hbaseClient = mock<HbaseClient>()
            val consumer = mock<KafkaConsumer<ByteArray, ByteArray>>()

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
                        on { offset() } doReturn recordNumber.toLong()
                    }
                }
            }

            recordProcessor.processRecords(hbaseClient, consumer, parser, ConsumerRecords<ByteArray, ByteArray>(map))

            val tableNameCaptor = argumentCaptor<String>()
            val recordCaptor = argumentCaptor<List<HbasePayload>>()
            verify(hbaseClient, times(10)).putList(tableNameCaptor.capture(), recordCaptor.capture())
            tableNameCaptor.allValues shouldBe (1..10).map { "database$it:collection$it" }

            val commitCaptor = argumentCaptor<Map<TopicPartition, OffsetAndMetadata>>()
            verify(consumer, times(10)).commitSync(commitCaptor.capture())

            commitCaptor.allValues.forEachIndexed { index, element ->
                val topicNumber = index + 1
                element.size shouldBe 1
                val topicPartition = TopicPartition("db.database$topicNumber.collection$topicNumber", 10 - topicNumber)
                element[topicPartition] shouldNotBe null
                element[topicPartition]?.offset() shouldBe 101
            }
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
