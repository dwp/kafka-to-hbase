class TextUtils {
    fun topicNameTableMatcher(topicName: String) = qualifiedTablePattern.find(topicName)
    private val qualifiedTablePattern = Regex("""^\w+\.([-\w]+)\.([-\w]+)$""")

    fun coalescedName(tableName: String) =
        if (coalescedNames[tableName] != null) coalescedNames[tableName] else tableName

    private val coalescedNames = mapOf("agentToDoArchive" to "agentToDo")

}
