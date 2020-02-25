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
import app.utils.TableNameUtil

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
        logger.info("Input received for topic '${input.topic}' with key '${input.key}' and timestamp '${input.timestamp}' for delete request '${input.deleteRequest}' and table per topic setting of '${input.useTablePerTopic}'")
        val timestamp = input.timestamp
        val isDeleteRequest = input.deleteRequest
        val useTablePerTopic = input.useTablePerTopic
        val family = (if (useTablePerTopic) "cf" else HbaseConfig.dataFamily)
        val column = (if (useTablePerTopic) "record" else input.topic)
        val tableName = tableNameUtil.getQualifiedTableName(input.topic, useTablePerTopic)
        if (isDeleteRequest) {
            logger.info("""Deleting messages from column '${family}:${column}',
                |table '${tableName}'""".trimMargin().replace("\n", " "))
            return deleteMessagesFromTopic(family.toByteArray(), column.toByteArray(), tableName, input.useTablePerTopic)
        } else {
            val formattedKey = keyGeneration.generateKey(input.key.toByteArray())
            val printableKey = keyGeneration.printableKey(formattedKey)

            logger.info("""Getting '$printableKey' 
                |column '${family}:${column}',
                |table '${tableName}',
                |timestamp '$timestamp'""".trimMargin().replace("\n", " "))

            // Connect to Hbase using configured values
            ConnectionFactory.createConnection(HBaseConfiguration.create(HbaseConfig.config)).use { connection ->
                with (TableName.valueOf(tableName)) {
                    if (connection.admin.tableExists(this)) {
                        connection.getTable(this).use { table ->
                            val result = table.get(Get(formattedKey).apply {
                                if (input.timestamp > 0) {
                                    setTimeStamp(timestamp)
                                }
                                addColumn(family.toByteArray(), column.toByteArray())
                            })
                            // Return the value of the cell directly, parsed as a UTF-8 string
                            return result.getValue(family.toByteArray(), column.toByteArray())
                        }
                    }
                    else {
                        logger.info("Table '$tableName' does not exist.")
                    }
                }
            }
        }

        return null
    }

    fun deleteMessagesFromTopic(dataFamily: ByteArray, dataQualifier: ByteArray, tableName: String, useTablePerTopic: Boolean): ByteArray? {
        ConnectionFactory.createConnection(HBaseConfiguration.create(HbaseConfig.config)).use { connection ->
            if (useTablePerTopic) {
                with (TableName.valueOf(tableName)) {
                    connection.admin.disableTable(this)
                    connection.admin.deleteTable(this)
                }
            }
            else {
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
        }
        return dataQualifier
    }
}
