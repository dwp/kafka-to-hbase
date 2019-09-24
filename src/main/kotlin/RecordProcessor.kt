import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.logging.Logger
import com.beust.klaxon.JsonObject
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

open class RecordProcessor() {
    fun processRecord(record: ConsumerRecord<ByteArray, ByteArray>, hbase: HbaseClient, parser: MessageParser, log: Logger) {
        var json: JsonObject
        val converter = Converter()

        try {
            json = converter.convertToJson(record.value())
        } catch (e: IllegalArgumentException) {
            log.warning("Could not parse message body for record with data of %s".format(
                getDataStringForRecord(record)
            )
            )
            sendMessageToDlq(record)
            return
        }

        val formattedKey = parser.generateKeyFromRecordBody(json)

        if (formattedKey.isEmpty()) {
            log.warning(
                "Empty key was skipped for record with data of %s".format(
                    getDataStringForRecord(record)
                ))
            return
        }

        try {
            val lastModifiedTimestampStr = converter.getLastModifiedTimestamp(json)
            val lastModifiedTimestampLong = converter.getTimestampAsLong(lastModifiedTimestampStr)
            hbase.putVersion(
                topic = record.topic().toByteArray(),
                key = formattedKey,
                body = record.value(),
                version = lastModifiedTimestampLong
            )
            log.info(
                "Written record to HBase with data of %s".format(
                    getDataStringForRecord(record)
                )
            )
        } catch (e: Exception) {
            log.severe(
                "Error writing record to HBase with data of %s".format(
                    getDataStringForRecord(record)
                )
            )
            throw e
        }
    }

    open fun sendMessageToDlq(record: ConsumerRecord<ByteArray, ByteArray>) {
        val body = record.value()
        val malformedRecord = MalformedRecord(body, "Not a valid json".toByteArray())
        try {
            val producerRecord = ProducerRecord(
                Config.Kafka.dlqTopic,
                null,
                System.currentTimeMillis(),
                record.key(),
                getObjectAsByteArray(malformedRecord),
                null
            )
            /*val callback = Callback { metadata, exception ->
                if (exception != null) {
                    throw RuntimeException(exception)
                } else {
                    log.info(""+Thread.currentThread())
                    log.info("${metadata}")
                }
            }
            kafka.producer.send(producerRecord, callback)*/
            kafka.producer.send(producerRecord)
        } catch (e: Exception) {
            log.warning(
                ("Error while sending message to dlq : " +
                    "key %s from topic %s with offset %s : %s").format(record.key(), record.topic(), record.offset(), e.toString()))
            throw DlqException("Exception while sending message to DLQ " + e)
        }
    }

    fun getObjectAsByteArray(obj: Any): ByteArray? {
        val bos = ByteArrayOutputStream()
        val oos = ObjectOutputStream(bos)
        oos.writeObject(obj)
        oos.flush()
        return bos.toByteArray()
    }
}

fun getDataStringForRecord(record: ConsumerRecord<ByteArray, ByteArray>): String {
    return "%s:%s:%d:%d".format(
        String(record.key() ?: ByteArray(0)),
        record.topic(),
        record.partition(),
        record.offset()
    )
}

