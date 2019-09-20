import java.util.logging.Logger
import com.beust.klaxon.JsonObject

open class MessageParser() {

    val convertor = Convertor()
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
        val jsonOrdered = convertor.sortJsonByKey(json)
        val checksumBytes: ByteArray = convertor.generateFourByteChecksum(jsonOrdered)
        
        return checksumBytes.plus(jsonOrdered.toByteArray())
    }
}