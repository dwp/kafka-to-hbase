package parser

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import java.text.SimpleDateFormat

class TimestampDeserializer : StdDeserializer<Long>(String::class.java) {
    val TIMESTAMP_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"

    override fun deserialize(p: JsonParser?, ctxt: DeserializationContext?): Long {
        val df = SimpleDateFormat(TIMESTAMP_PATTERN);
        val date = p?.text
        return df.parse(date).time
    }
}