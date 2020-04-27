import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Scan
import app.utils.TableNameUtil
import org.slf4j.LoggerFactory
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Suppress("unused")
class Handler : RequestHandler<Request, ByteArray?> {
    private val logger: DataworksLogger = DataworksLogger(LoggerFactory.getLogger(Handler::class.java))
    private val keyGeneration = KeyGeneration()
    private val tableNameUtil = TableNameUtil()

    override fun handleRequest(input: Request, context: Context?): ByteArray? {
        logger.info("Input received", "input_topic" to input.topic, "input_key" to input.key, 
            "input_timestamp" to input.timestamp.toString(), "is_delete_request" to input.deleteRequest.toString(), 
             "is_table_per_topic" to input.useTablePerTopic.toString())

        val timestamp = input.timestamp
        val isDeleteRequest = input.deleteRequest
        val useTablePerTopic = input.useTablePerTopic
        val family = (if (useTablePerTopic) "cf" else HbaseConfig.dataFamily)
        val column = (if (useTablePerTopic) "record" else input.topic)
        val tableName = tableNameUtil.getQualifiedTableName(input.topic, useTablePerTopic)
        if (isDeleteRequest) {
            logger.info("Deleting messages from HBase", "hbase_family" to family, 
                "hbase_column" to column, "hbase_table_name" to tableName)
            return deleteMessagesFromTopic(family.toByteArray(), column.toByteArray(), tableName, input.useTablePerTopic)
        } else {
            val formattedKey = keyGeneration.generateKey(input.key.toByteArray())
            val printableKey = keyGeneration.printableKey(formattedKey)

            logger.info("Retrieving data", "printable_key" to printableKey, 
                "hbase_family" to family, "hbase_column" to column,
                "hbase_table_name" to tableName, "hbase_timestamp" to timestamp.toString())

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
                        logger.error("Table does not exist", "hbase_table_name" to tableName)
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
                    if (connection.admin.tableExists(this)) {
                        connection.admin.disableTable(this)
                        connection.admin.deleteTable(this)
                    }
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
