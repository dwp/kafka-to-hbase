import java.io.Serializable

data class MalformedRecord(val body: String, val reason: String) : Serializable

