class TextUtils {

    private val qualifiedTablePattern = Regex(Config.Hbase.qualifiedTablePattern)
    private val coalescedNames = mapOf("agent_core:agentToDoArchive" to "agent_core:agentToDo")

    fun printableKey(key: ByteArray) =
        if (key.size > 4) {
            val hash = key.slice(IntRange(0, 3))
            val hex = hash.joinToString("") { String.format("\\x%02X", it) }
            val renderable = key.slice(IntRange(4, key.size - 1)).map { it.toChar() }.joinToString("")
            "${hex}${renderable}"
        }
        else {
            String(key)
        }

    fun topicNameTableMatcher(topicName: String) = qualifiedTablePattern.find(topicName)

    fun coalescedName(tableName: String) =
        if (coalescedNames[tableName] != null) coalescedNames[tableName] else tableName
}
