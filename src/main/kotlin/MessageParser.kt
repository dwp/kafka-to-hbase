import com.beust.klaxon.JsonObject
import com.beust.klaxon.JsonValue
import java.util.logging.Logger

open class MessageParser {

    private val converter = Converter()
    private val log: Logger = Logger.getLogger("messageParser")

    open fun generateKeyFromRecordBody(body: JsonObject?): ByteArray {
        val id: JsonObject? = body?.let { getId(it) }
        return if (id == null) ByteArray(0) else generateKey(id)
    }

    fun getId(json: JsonObject): JsonObject? {
        val message: JsonObject? = json.obj("message")
        if (message != null) {
            val id = message.get("_id")
            if (id != null && id is JsonValue) {
                if (id.obj != null) {
                    return id.obj
                }
                else if (id.string != null) {
                    val idObject = JsonObject()
                    idObject["id"] = id.string
                    return idObject
                }
                else if (id.int != null) {
                    val idObject = JsonObject()
                    idObject["id"] = "${id.int}"
                    return idObject
                }
                else {
                    return null
                }
            }
            else {
                return null
            }

        }
        else {
            return null
        }
    }

    fun generateKey(json: JsonObject): ByteArray {
        val jsonOrdered = converter.sortJsonByKey(json)
        val checksumBytes: ByteArray = converter.generateFourByteChecksum(jsonOrdered)

        return checksumBytes.plus(jsonOrdered.toByteArray())
    }
}
