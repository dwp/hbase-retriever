import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Connection
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
            "is_delete_entire_table" to input.useTablePerTopic.toString())

        val timestamp = input.timestamp
        val isDeleteRequest = input.deleteRequest
        val deleteEntireTable = input.useTablePerTopic
        val key = input.key

        val family = HbaseConfig.dataFamily
        val column = HbaseConfig.dataColumn

        val tableName = tableNameUtil.getQualifiedTableName(input.topic)

        val hbaseConnection = getHbaseConnection()

        if (isDeleteRequest) {
            logger.info("Deleting messages from HBase", "hbase_family" to family, 
                "hbase_column" to column, "hbase_table_name" to tableName)

            return deleteMessagesFromTopic(hbaseConnection, family.toByteArray(), column.toByteArray(), tableName, deleteEntireTable)
        } 
        else {
            val formattedKey = keyGeneration.generateKey(key.toByteArray())
            val printableKey = keyGeneration.printableKey(formattedKey)

            logger.info("Retrieving data", "printable_key" to printableKey, 
                "hbase_family" to family, "hbase_column" to column,
                "hbase_table_name" to tableName, "hbase_timestamp" to timestamp.toString())

            return getDataFromHbase(hbaseConnection, tableName, formattedKey, family, column, timestamp)
        }
    }

    fun getDataFromHbase(hbaseConnection: Connection, tableName: String, formattedKey: ByteArray, family: String, column: String, timestamp: Long): ByteArray? {
        hbaseConnection.use { connection ->
            with (TableName.valueOf(tableName)) {
                if (connection.admin.tableExists(this)) {
                    connection.getTable(this).use { table ->
                        val result = table.get(Get(formattedKey).apply {
                            if (timestamp > 0) {
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
        return null
    }

    fun deleteMessagesFromTopic(connection: Connection, dataFamily: ByteArray, dataQualifier: ByteArray, tableName: String, deleteEntireTable: Boolean): ByteArray? {
        with(connection) {
            if (deleteEntireTable) {
                logger.info("Deleting entire table", "hbase_family" to String(dataFamily),
                    "hbase_table_name" to tableName, "hbase_qualifier" to String(dataQualifier))

                with (TableName.valueOf(tableName)) {
                    if (connection.admin.tableExists(this)) {
                        connection.admin.disableTable(this)
                        connection.admin.deleteTable(this)
                    }
                }
            }
            else {
                logger.info("Clearing table", "hbase_family" to String(dataFamily),
                    "hbase_table_name" to tableName, "hbase_qualifier" to String(dataQualifier))

                connection.getTable(TableName.valueOf(tableName)).use { table ->
                    val deleteList = mutableListOf<Delete>()
                    val scan = Scan()
                    val scanner = table.getScanner(scan)
                    scanner.forEach { result ->
                        val rowKey = result.row
                        val delete = Delete(rowKey).addColumns(dataFamily, dataQualifier)
                        deleteList.add(delete)
                    }
                    table.delete(deleteList)
                }
            }
        }
        return dataQualifier
    }

    fun getHbaseConnection(): Connection {
        return ConnectionFactory.createConnection(HBaseConfiguration.create(HbaseConfig.config))
    }
}
