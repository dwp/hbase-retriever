package app.utils

import org.slf4j.LoggerFactory
import uk.gov.dwp.dataworks.logging.DataworksLogger

class TableNameUtil {
    private val logger: DataworksLogger = DataworksLogger(LoggerFactory.getLogger(TableNameUtil::class.java))
    private val regexPattern = """^\w+\.([-\w]+)\.([-\w]+)$"""
    private val qualifiedTablePattern = Regex(regexPattern)

    fun topicNameTableMatcher(topicName: String) = qualifiedTablePattern.find(topicName)

    fun getQualifiedTableName(topic: String): String {
        val matcher = topicNameTableMatcher(topic)
        if (matcher != null) {
            val namespace = matcher.groupValues[1]
            val tableName = matcher.groupValues[2]
            val qualified_table_name = "$namespace:$tableName".replace("-", "_")
            logger.info("Successfully parsed table name from topic", 
                "topic" to topic, "regex_pattern" to regexPattern, 
                "qualified_table_name" to qualified_table_name)
            return qualified_table_name
        }
        else {
            logger.error("Could not parse table name from topic", 
                "topic" to topic, "regex_pattern" to regexPattern)
            throw Exception("Could not parse table name from topic: '${topic}'")
        }
    }
}