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

    private val logger = Logger.getLogger("Handler")
    private val keyGeneration = KeyGeneration()
    private val tableNameUtil = TableNameUtil()

    override fun handleRequest(input: Request, context: Context?): ByteArray? {
        logger.info("Input received for topic ${input.topic} with key ${input.key} and timestamp ${input.timestamp} for delete request ${input.deleteRequest}")
        val topic = input.topic.toByteArray()
        val timestamp = input.timestamp
        val isDeleteRequest = input.deleteRequest
        val useTablePerTopic = input.useTablePerTopic
        val family = HbaseConfig.dataFamily.toByteArray()
        val tableName = tableNameUtil.getQualifiedTableName(topic, useTablePerTopic)
        if (isDeleteRequest) {
            logger.info("Deleting messages from topic '${input.topic}'.")
            return deleteMessagesFromTopic(family, topic, tableName)
        } else {
            val formattedKey = keyGeneration.generateKey(input.key.toByteArray())
            val printableKey = keyGeneration.printableKey(formattedKey)

            logger.info("""Getting '$printableKey' 
                |column '${HbaseConfig.dataFamily}:${input.topic}',
                |timestamp '$timestamp'""".trimMargin().replace("\n", " "))

            // Connect to Hbase using configured values
            ConnectionFactory.createConnection(HBaseConfiguration.create(HbaseConfig.config)).use { connection ->
                connection.getTable(TableName.valueOf(tableName)).use { table ->
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

    fun deleteMessagesFromTopic(dataFamily: ByteArray, dataQualifier: ByteArray, tableName: String): ByteArray? {
        ConnectionFactory.createConnection(HBaseConfiguration.create(HbaseConfig.config)).use { connection ->
            connection.getTable(TableName.valueOf(tableName)).use { table ->
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
