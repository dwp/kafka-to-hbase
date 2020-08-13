import com.nhaarman.mockitokotlin2.*
import io.kotlintest.specs.StringSpec
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer


class ListProcessorTest : StringSpec() {

    init {

        "works" {
            val validator = mock<Validator>()
            val recordProcessor = ListProcessor(validator, Converter())

            val hbaseClient = mock<HbaseClient> {
                on { ensureTable(any())} doAnswer { println("Creating table") }
            }

            val consumer = mock<KafkaConsumer<ByteArray, ByteArray>> {

            }

            val parser = mock<MessageParser> {
                val hbaseKeys = (1 .. 100).map {Bytes.toBytes(it)}
                on {generateKeyFromRecordBody(any())} doReturnConsecutively hbaseKeys
            }

            val records = mock<ConsumerRecords<ByteArray, ByteArray>> {

            }
//            val records2 = (1..100).map {
//                val body = Bytes.toBytes(json(it))
//                val key = Bytes.toBytes(it)
//                mock<ConsumerRecord<ByteArray, ByteArray>> {
//                    on { value() } doReturn body
//                    on { key() } doReturn key
//                }
//            }


            recordProcessor.processList(hbaseClient, consumer, parser, records)
        }
    }

    fun json(id: Any) =
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
