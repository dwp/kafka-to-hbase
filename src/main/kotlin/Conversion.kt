import java.util.logging.Logger
import java.util.zip.CRC32
import java.nio.ByteBuffer
import com.beust.klaxon.Parser
import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException
import com.beust.klaxon.lookup
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.Callback
import java.lang.RuntimeException
import java.text.SimpleDateFormat
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.ByteArrayOutputStream
import java.util.*
import java.io.ObjectOutputStream
import java.lang.IllegalArgumentException


open class Converter() {
    private val log: Logger = Logger.getLogger("Converter")

    fun convertToJson(record: ConsumerRecord<ByteArray, ByteArray>): JsonObject {
        val body = record.value()
        try {
            val parser: Parser = Parser.default()
            val stringBuilder: StringBuilder = StringBuilder(String(body))
            val json: JsonObject = parser.parse(stringBuilder) as JsonObject
            return json
        } catch (e: KlaxonException) {
             log.warning(
                    "Error while parsing message with key %s from topic %s with offset %s  into json: %s".format(record.key(),record.topic(),record.offset(), e.toString()))
            sendMessageToDlq(record)
            throw IllegalArgumentException("Cannot parse invalid JSON")
        }
    }

    open  fun sendMessageToDlq(record: ConsumerRecord<ByteArray, ByteArray>) {
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
                    "key %s from topic %s with offset %s : %s").format(record.key(),record.topic(),record.offset(),e.toString()))
            throw DlqException("Exception while sending message to DLQ " + e)
        }
    }

    fun sortJsonByKey(unsortedJson: JsonObject): String {
        val sortedEntries = unsortedJson.toSortedMap(compareBy<String> { it })
        val json: JsonObject = JsonObject(sortedEntries)
        
        return json.toJsonString()
    }

    fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()

        checksum.update(bytes, 0, bytes.size)

        return ByteBuffer.allocate(4).putInt(checksum.getValue().toInt()).array();
    }

    fun encodeToBase64(input: String): String {
        return Base64.getEncoder().encodeToString(input.toByteArray());
    }

    fun decodeFromBase64(input: String): String {
        val decodedBytes: ByteArray = Base64.getDecoder().decode(input);
        return String(decodedBytes);
    }

    fun getTimestampAsLong(timeStampAsStr: String?, timeStampPattern: String = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"): Long {
        val df = SimpleDateFormat(timeStampPattern);
        return df.parse(timeStampAsStr).time
    }

    fun getLastModifiedTimestamp(json: JsonObject): String? {
        val lastModifiedTimestampStr = json.lookup<String?>("message._lastModifiedDateTime").get(0)
        if (lastModifiedTimestampStr.isNullOrBlank()) throw RuntimeException("Last modified date time is null or blank")
        return lastModifiedTimestampStr
    }

    fun getObjectAsByteArray(obj : Any): ByteArray? {
        val bos = ByteArrayOutputStream()
        val oos = ObjectOutputStream(bos)
        oos.writeObject(obj)
        oos.flush()
        return bos.toByteArray()
    }
}
