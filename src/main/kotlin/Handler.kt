import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Connection
import app.utils.TableNameUtil
import app.utils.KeyGenerationUtil
import org.slf4j.LoggerFactory
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Suppress("unused")
class Handler : RequestHandler<Request, ByteArray?> {
    private val logger: DataworksLogger = DataworksLogger(LoggerFactory.getLogger(Handler::class.java))

    override fun handleRequest(input: Request, context: Context?): ByteArray? {
        logger.info("Input received", "input_topic" to input.topic, "input_key" to input.key, 
            "input_timestamp" to input.timestamp.toString(), "is_delete_request" to input.deleteRequest.toString(), 
            "is_delete_entire_table" to input.useTablePerTopic.toString())

        val keyGeneration = KeyGenerationUtil()
        val tableNameUtil = TableNameUtil()
        val hbaseManager = HbaseManager()
        val hbaseConnection = getHbaseConnection()

        val family = HbaseConfig.dataFamily
        val column = HbaseConfig.dataColumn

        return processRequest(hbaseConnection, input, keyGeneration, tableNameUtil, hbaseManager, family, column)
    }

    fun processRequest(connection: Connection, input: Request, keyGeneration: KeyGenerationUtil, tableNameUtil: TableNameUtil, hbaseManager: HbaseManager, family: String, column: String): ByteArray? {
        val timestamp = input.timestamp
        val isDeleteRequest = input.deleteRequest
        val deleteEntireTable = input.useTablePerTopic
        val key = input.key

        val tableName = tableNameUtil.getQualifiedTableName(input.topic)

        if (isDeleteRequest) {
            logger.info("Deleting messages from HBase", "hbase_family" to family, 
                "hbase_column" to column, "hbase_table_name" to tableName)

            return hbaseManager.deleteMessagesFromTopic(connection, family.toByteArray(), column.toByteArray(), tableName, deleteEntireTable)
        } 
        else {
            val formattedKey = keyGeneration.generateKey(key.toByteArray())
            val printableKey = keyGeneration.printableKey(formattedKey)

            logger.info("Retrieving data", "printable_key" to printableKey, 
                "hbase_family" to family, "hbase_column" to column,
                "hbase_table_name" to tableName, "hbase_timestamp" to timestamp.toString())

            return hbaseManager.getDataFromHbase(connection, tableName, formattedKey, family.toByteArray(), column.toByteArray(), timestamp)
        }
    }

    fun getHbaseConnection(): Connection {
        return ConnectionFactory.createConnection(HBaseConfiguration.create(HbaseConfig.config))
    }
}
