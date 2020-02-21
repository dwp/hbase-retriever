package app.utils

class TableNameUtil {
    private val qualifiedTablePattern = Regex("""^\w+\.([-\w]+)\.([-\w]+)$""")

    fun topicNameTableMatcher(topicName: String) = qualifiedTablePattern.find(topicName)

    fun getQualifiedTableName(topic: String, useTablePerTopic: Boolean): String {
        if (useTablePerTopic) {
            val matcher = topicNameTableMatcher(topic)
            if (matcher != null) {
                val namespace = matcher.groupValues[1]
                val tableName = matcher.groupValues[2]
                return "$namespace:$tableName".replace("-", "_")
            }
        }
        
        return HbaseConfig.dataTable
    }
}