import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.logging.Logger
import com.beust.klaxon.JsonObject

class RecordProcessor() {
    fun processRecord(record: ConsumerRecord<ByteArray, ByteArray>, hbase: HbaseClient, parser: MessageParser, log: Logger) {
        var json: JsonObject
        val convertor = Convertor()

        try {
            json = convertor.convertToJson(record.value())
        } catch (e: IllegalArgumentException) {
            log.warning("Could not parse message body, record will be skipped")
            return
        }

        val formattedKey = parser.generateKeyFromRecordBody(json)

        if (formattedKey.isEmpty()) {
            log.warning(
                "Empty key was skipped for %s:%d:%d".format(
                    record.topic() ?: "null",
                    record.partition(),
                    record.offset()
                ))
            return
        }

        try {
            val lastModifiedTimestampStr = convertor.getLastModifiedTimestamp(json)
            val lastModifiedTimestampLong = convertor.getTimestampAsLong(lastModifiedTimestampStr)
            hbase.putVersion(
                topic = record.topic().toByteArray(),
                key = formattedKey,
                body = record.value(),
                version = lastModifiedTimestampLong
            )
            log.info(
                "Wrote key %s data %s:%d:%d".format(
                    String(formattedKey),
                    record.topic() ?: "null",
                    record.partition(),
                    record.offset()
                )
            )
        } catch (e: Exception) {
            log.severe(
                "Error while writing key %s data %s:%d:%d: %s".format(
                    String(formattedKey),
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
