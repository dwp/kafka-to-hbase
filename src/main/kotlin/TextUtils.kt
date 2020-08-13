class TextUtils {

    private val qualifiedTablePattern = Regex(Config.Hbase.qualifiedTablePattern)
    private val coalescedNames = mapOf("agent_core:agentToDoArchive" to "agent_core:agentToDo")

    fun topicNameTableMatcher(topicName: String) = qualifiedTablePattern.find(topicName)

    fun qualifiedTableName(topic: String): String? {
        val matcher = topicNameTableMatcher(topic)
        return if (matcher != null) {
            val namespace = matcher.groupValues[1]
            val tableName = matcher.groupValues[2]
            targetTable(namespace, tableName)
        }
        else null
    }

    private fun targetTable(namespace: String, tableName: String) =
        coalescedName("$namespace:$tableName")?.replace("-", "_")

    fun coalescedName(tableName: String) =
        if (coalescedNames[tableName] != null) coalescedNames[tableName] else tableName
}
