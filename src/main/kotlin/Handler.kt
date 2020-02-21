import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Scan
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout

@Suppress("unused")
class Handler : RequestHandler<Request, ByteArray?> {
    init {
        val consoleAppender = ConsoleAppender()
        consoleAppender.layout = PatternLayout("%d [%p|%c|%C{1}] %m%n")
        consoleAppender.threshold = Level.INFO
        consoleAppender.activateOptions()
        Logger.getRootLogger().addAppender(consoleAppender)
    }

    val logger = Logger.getLogger("Handler")
    val keyGeneration = KeyGeneration()

    override fun handleRequest(input: Request, context: Context?): ByteArray? {
        logger.info("Input received for topic ${input.topic} with key ${input.key} and timestamp ${input.timestamp} for delete request ${input.deleteRequest}")
        val topic = input.topic.toByteArray()
        val timestamp = input.timestamp
        val isDeleteRequest = input.deleteRequest
        val useTablePerTopic = input.useTablePerTopic
        val family = HbaseConfig.dataFamily.toByteArray()
        if (isDeleteRequest) {
            logger.info("Deleting messages from topic '${input.topic}'.")
            return deleteMessagesFromTopic(family, topic)
        } else {
            val formattedKey = keyGeneration.generateKey(input.key.toByteArray())
            val printableKey = keyGeneration.printableKey(formattedKey)

            logger.info("""Getting '$printableKey' 
                |column '${HbaseConfig.dataFamily}:${input.topic}',
                |timestamp '$timestamp'""".trimMargin().replace("\n", " "))

            // Connect to Hbase using configured values
            ConnectionFactory.createConnection(HBaseConfiguration.create(HbaseConfig.config)).use { connection ->
                connection.getTable(TableName.valueOf(HbaseConfig.dataTable)).use { table ->
                    val result = table.get(Get(formattedKey).apply {
                        if (input.timestamp > 0) {
                            setTimeStamp(timestamp)
                        }
                        addColumn(family, topic)
                    })
                    // Return the value of the cell directly, parsed as a UTF-8 string
                    return result.getValue(family, topic)
                }
            }
        }
    }

    fun getQualifiedTableName(topic: String, useTablePerTopic: Boolean): String {
        if (!useTablePerTopic) {
            return HbaseConfig.dataTable
        }

        val matcher = textUtils.topicNameTableMatcher(topicName)
            if (matcher != null) {
                val namespace = matcher.groupValues[1]
                val tableName = matcher.groupValues[2]
                val qualifiedTableName = "$namespace:$tableName".replace("-", "_")
                val table = connection.getTable(TableName.valueOf(qualifiedTableName))
                scanner = table.getScanner(scan())
            }
    }

    fun deleteMessagesFromTopic(dataFamily: ByteArray, dataQualifier: ByteArray): ByteArray? {

        ConnectionFactory.createConnection(HBaseConfiguration.create(HbaseConfig.config)).use { connection ->
            connection.getTable(TableName.valueOf(HbaseConfig.dataTable)).use { table ->
                val deleteList = mutableListOf<Delete>()
                val scan = Scan()
                val scanner = table.getScanner(scan)
                scanner.forEach { result ->
                    val rowKey = result.row
                    // Deletes all versions of a given family and qualifier for a given row key
                    val delete = Delete(rowKey).addColumns(dataFamily, dataQualifier)
                    deleteList.add(delete)
                }
                table.delete(deleteList)
            }

            connection.getTable(TableName.valueOf(HbaseConfig.topicTable)).use { table ->
                val delete = Delete(dataQualifier)
                table.delete(delete)
            }
        }
        return dataQualifier
    }
}


