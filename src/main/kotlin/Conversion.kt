import org.bson.BsonDocument
import kotlin.js.json
import java.util.logging.Logger
import java.security.MessageDigest
import javax.xml.bind.DatatypeConverter

fun convertToJson(body: ByteArray) {
    val log = Logger.getLogger("generateKey")

    try {
        return JSON.parse<Json>(String(body))
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

fun convertToBson(input: Json) {
    val input_string = JSON.stringify(input)
    return BsonDocument.parse(input_string)
}

fun generateHash(type: String, input: String): String {
    val bytes = MessageDigest
            .getInstance(type)
            .digest(input.toByteArray())
    return DatatypeConverter.printHexBinary(bytes).toUpperCase()
}