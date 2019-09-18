import org.bson.BsonDocument
import java.util.logging.Logger
import java.util.Base64
import java.security.MessageDigest
import javax.xml.bind.DatatypeConverter
import com.beust.klaxon.Parser
import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException

fun convertToJson(body: ByteArray): JsonObject {
    val log = Logger.getLogger("convertToJson")

    try {
        val parser: Parser = Parser.default()
        val stringBuilder: StringBuilder = StringBuilder(String(body))
        val json: JsonObject = parser.parse(stringBuilder) as JsonObject
        return json
    } catch (e: KlaxonException) {
        log.warning(
            "Error while parsing message body of '%s' in to json: %s".format(
                String(body),
                e.toString()
            )
        )
        throw IllegalArgumentException("Cannot parse invalid JSON")
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

fun encodeToBase64(input: String): String {
    return Base64.getEncoder().encodeToString(input.toByteArray());
}

fun decodeFromBase64(input: String): String {
    val decodedBytes: ByteArray = Base64.getDecoder().decode(input);
    return String(decodedBytes);
}
