import org.bson.BsonDocument
import java.util.logging.Logger
import java.util.Base64
import java.util.zip.CRC32
import java.util.zip.Checksum
import java.nio.ByteBuffer
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

fun sortJsonByKey(unsortedJson: JsonObject): String {
    val sortedEntries = unsortedJson.toSortedMap(compareBy<String> { it.toLowerCase() })
    val sortedEntriesString = sortedEntries.entries.joinToString(",").replace("[", "").replace("]", "")
    return sortedEntriesString
}

fun convertToBson(text: String): BsonDocument {
    return BsonDocument.parse(text)
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
