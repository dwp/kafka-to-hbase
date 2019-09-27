import com.beust.klaxon.JsonObject
import com.nhaarman.mockitokotlin2.*
import io.kotlintest.TestCase
import io.kotlintest.fail
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockkObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.junit.Assert.assertEquals
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.time.Duration
import java.util.concurrent.Future
import java.util.logging.Logger


class RecordProcessorTest : StringSpec() {

    lateinit var mockValidator: Validator
    lateinit var mockConverter: Converter
    lateinit var mockMessageParser: MessageParser
    lateinit var hbaseClient: HbaseClient
    lateinit var logger: Logger
    lateinit var processor: RecordProcessor
    val testByteArray: ByteArray = byteArrayOf(0xA1.toByte(), 0xA1.toByte(), 0xA1.toByte(), 0xA1.toByte())

    override fun beforeTest(testCase: TestCase) {
        super.beforeTest(testCase)
        println("Before every spec/test case")
        mockValidator = mock()
        mockConverter = mock()
        mockMessageParser = mock()
        hbaseClient = mock()
        logger = mock()
        processor = spy(RecordProcessor(mockValidator, mockConverter))
    }

    init {
        configureLogging()
        doNothing().whenever(mockValidator).validate(any())
        doNothing().whenever(processor).sendMessageToDlq(any(), any())

        "valid record is sent to hbase successfully" {
            val messageBody = """{
        "message": {
           "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
           "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
        }
    }"""
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)

            processor.processRecord(record, hbaseClient, mockMessageParser, logger)

            verify(hbaseClient).putVersion("testTopic".toByteArray(), testByteArray, messageBody.toByteArray(), 1544799662000)
            verify(logger).info(any<String>())
        }

        "record value with invalid json is not sent to hbase" {
            val messageBody = """{"message":{"_id":{"test_key_a":,"test_key_b":"test_value_b"}}}"""
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 111, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)
            processor.processRecord(record, hbaseClient, mockMessageParser, logger)

            verifyZeroInteractions(hbaseClient)
        }

        "Exception should be thrown when dlq topic is not available and message  is not sent to hbase" {
            mockkObject(DlqProducer)
            val obj = object : org.apache.kafka.clients.producer.Producer<ByteArray, ByteArray> {
                override fun partitionsFor(topic: String?): MutableList<PartitionInfo> {
                    throw Exception("")
                }

                override fun flush() {
                }

                override fun abortTransaction() {
                }

                override fun commitTransaction() {
                }

                override fun beginTransaction() {
                }

                override fun initTransactions() {
                }

                override fun sendOffsetsToTransaction(offsets: MutableMap<TopicPartition, OffsetAndMetadata>?, consumerGroupId: String?) {
                }

                override fun send(record: ProducerRecord<ByteArray, ByteArray>?): Future<RecordMetadata> {
                    throw Exception("")
                }

                override fun send(record: ProducerRecord<ByteArray, ByteArray>?, callback: Callback?): Future<RecordMetadata> {
                    throw Exception("")
                }

                override fun close() {
                }

                override fun close(timeout: Duration?) {
                }

                override fun metrics(): MutableMap<MetricName, out Metric> {
                    throw Exception("")
                }
            }
            every { DlqProducer.getInstance() } returns obj
            val processor = RecordProcessor(mockValidator, mockConverter)
            val messageBody = """{"message":{"_id":{"test_key_a":,"test_key_b":"test_value_b"}}}"""
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 111, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)

            shouldThrow<DlqException> {
                processor.processRecord(record, hbaseClient, mockMessageParser, logger)
            }

            verifyZeroInteractions(hbaseClient)
            verify(logger, times(2)).warning(any<String>())
        }

        "record value with invalid _id field is not sent to hbase" {
            val messageBody = """{
        "message": {
           "id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
           "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
        }
    }"""
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(ByteArray(0))

            processor.processRecord(record, hbaseClient, mockMessageParser, logger)

            verifyZeroInteractions(hbaseClient)
            verify(logger, atLeastOnce()).warning(any<String>())
        }

        "exception in hbase communication causes severe log message" {
            val messageBody = """{
        "message": {
           "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
           "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
        }
    }"""
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)

            whenever(hbaseClient.putVersion("testTopic".toByteArray(), testByteArray, messageBody.toByteArray(), 1544799662000)).doThrow(RuntimeException("testException"))

            try {
                processor.processRecord(record, hbaseClient, mockMessageParser, logger)
                fail("test did not throw an exception")
            } catch (e: RuntimeException) {
                verify(logger, atLeastOnce()).severe(any<String>())
            }
        }

        "Malformed record object can be converted to bytearray " {
            val malformedRecord = MalformedRecord("key", "junk", "Not a valid json")
            val byteArray = processor.getObjectAsByteArray(malformedRecord)
            val bi = ByteArrayInputStream(byteArray)
            val oi = ObjectInputStream(bi)
            val actual = oi.readObject()
            assertEquals(malformedRecord, actual)
        }

        "Json that fails schema validation is sent to the dlq" {
            val messageBody = "Hello everyone"
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, "key".toByteArray(), messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)
            val jsonObject = JsonObject()
            whenever(mockConverter.convertToJson(record.value())).thenReturn(jsonObject)
            whenever(mockValidator.validate(jsonObject.toJsonString())).thenThrow(InvalidMessageException("oops!!", Exception()))

            processor.processRecord(record, hbaseClient, mockMessageParser, logger)

            verifyZeroInteractions(hbaseClient)
            verify(processor, times(1)).sendMessageToDlq(eq(record), eq("Invalid schema for key:testTopic:1:11: oops!!"))
            verify(logger).warning(any<String>())
        }

        "Invalid Json that fails parsing is sent to the dlq" {
            val messageBody = "Hello everyone"
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, "key".toByteArray(), messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)
            whenever(mockConverter.convertToJson(record.value())).thenThrow(IllegalArgumentException())

            processor.processRecord(record, hbaseClient, mockMessageParser, logger)

            verifyZeroInteractions(hbaseClient)
            verify(processor, times(1)).sendMessageToDlq(eq(record), eq("Invalid json"))
            verifyZeroInteractions(mockValidator)
            verify(logger).warning(any<String>())
        }
    }

}