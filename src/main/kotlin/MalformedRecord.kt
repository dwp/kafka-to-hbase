import java.io.Serializable

data class MalformedRecord (val body: ByteArray, val reason:ByteArray):Serializable{
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MalformedRecord

        if (!body.contentEquals(other.body)) return false
        if (!reason.contentEquals(other.reason)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = body.contentHashCode()
        result = 31 * result + reason.contentHashCode()
        return result
    }

}