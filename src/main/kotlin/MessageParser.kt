import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.logging.Logger
import com.beust.klaxon.JsonObject

open class MessageParser() {

    val log = Logger.getLogger("messageParser")

    open fun generateKeyFromRecordBody(body: JsonObject): ByteArray {
        val id: JsonObject? = getId(body)
        return if (id == null) ByteArray(0) else generateKey(id)
    }

    fun getId(json: JsonObject): JsonObject? {
        try {
            val message: JsonObject? = json.obj("message")
            return if (message == null) null else message.obj("_id")
        } catch (e: ClassCastException) {
            log.warning("Record body does not contain valid json object at message._id")
            return null
        }
    }

    fun generateKey(json: JsonObject): ByteArray {
        val jsonOrdered = sortJsonByKey(json)
        val base64EncodedString: String = encodeToBase64(jsonOrdered)
        val checksumBytes: ByteArray = generateFourByteChecksum(jsonOrdered)
        
        return checksumBytes.plus(base64EncodedString.toByteArray())
    }
}