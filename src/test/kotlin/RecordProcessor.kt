import io.kotlintest.fail
import io.kotlintest.specs.StringSpec
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.atLeastOnce
import com.nhaarman.mockitokotlin2.whenever
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.doThrow
import com.nhaarman.mockitokotlin2.verifyZeroInteractions
import java.util.logging.Logger
import com.beust.klaxon.JsonObject

class RecordProcessorTest : StringSpec({
    configureLogging()

    val processor = RecordProcessor()
    val testByteArray: ByteArray = byteArrayOf(0xA1.toByte(), 0xA1.toByte(), 0xA1.toByte(), 0xA1.toByte())
    val hbaseClient = mock<HbaseClient>()
    val logger = mock<Logger>()

    "valid record is sent to hbase successfully" {
        val messageBody: ByteArray = "{\"message\":{\"_id\":{\"test_key_a\":\"test_value_a\",\"test_key_b\":\"test_value_b\"}}}".toByteArray()
        val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 111, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody)
        val parser = mock<MessageParser> {
            on { generateKeyFromRecordBody(any<JsonObject>()) }.doReturn(testByteArray)
        }

        processor.processRecord(record, hbaseClient, parser, logger)

        verify(hbaseClient).putVersion("testTopic".toByteArray(), testByteArray, messageBody, 111)
        verify(logger).info(any<String>())
    }

    "record value with invalid json is not sent to hbase" {
        val messageBody: ByteArray = "{\"message\":{\"_id\":{\"test_key_a\":,\"test_key_b\":\"test_value_b\"}}}".toByteArray()
        val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 111, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody)
        val parser = mock<MessageParser> {
            on { generateKeyFromRecordBody(any<JsonObject>()) }.doReturn(testByteArray)
        }

        processor.processRecord(record, hbaseClient, parser, logger)

        verifyZeroInteractions(hbaseClient)
        verify(logger).warning(any<String>())
    }

    "record value with invalid _id field is not sent to hbase" {
        val messageBody: ByteArray = "{\"message\":{\"id\":{\"test_key_a\":\"test_value_a\",\"test_key_b\":\"test_value_b\"}}}".toByteArray()
        val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 111, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody)
        val parser = mock<MessageParser> {
            on { generateKeyFromRecordBody(any<JsonObject>()) }.doReturn(ByteArray(0))
        }

        processor.processRecord(record, hbaseClient, parser, logger)

        verifyZeroInteractions(hbaseClient)
        verify(logger, atLeastOnce()).warning(any<String>())
    }

    "exception in hbase communication causes severe log message" {
        val messageBody: ByteArray = "{\"message\":{\"_id\":{\"test_key_a\":\"test_value_a\",\"test_key_b\":\"test_value_b\"}}}".toByteArray()
        val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("testTopic", 1, 11, 111, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody)
        val parser = mock<MessageParser> {
            on { generateKeyFromRecordBody(any<JsonObject>()) }.doReturn(testByteArray)
        }

        whenever(hbaseClient.putVersion("testTopic".toByteArray(), testByteArray, messageBody, 111)).doThrow(RuntimeException("testException"))
        
        try {
            processor.processRecord(record, hbaseClient, parser, logger)
            fail("test did not throw an exception")
        } catch (e: RuntimeException) {
            verify(logger).severe(any<String>())
        }
    }
})