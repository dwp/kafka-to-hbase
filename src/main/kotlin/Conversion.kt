import org.bson.BsonDocument
import java.util.logging.Logger
import java.security.MessageDigest
import javax.xml.bind.DatatypeConverter
import com.beust.klaxon.Parser
import com.beust.klaxon.JsonObject

fun convertToJson(body: ByteArray): JsonObject {
    val log = Logger.getLogger("generateKey")

    try {
        val parser: Parser = Parser.default()
        val stringBuilder: StringBuilder = StringBuilder(String(body))
        val json: JsonObject = parser.parse(stringBuilder) as JsonObject
        return json
    } catch (e: Exception) {
        log.severe(
            "Error while parsing message body of '%s' in to json: %s".format(
                String(body),
                e.toString()
            )
        )
        throw e
    }
}

fun convertToBson(input: JsonObject): BsonDocument {
    return BsonDocument.parse(input.toJsonString())
}

fun generateHash(type: String, input: String): String {
    val bytes = MessageDigest
            .getInstance(type)
            .digest(input.toByteArray())
    return DatatypeConverter.printHexBinary(bytes).toUpperCase()
}