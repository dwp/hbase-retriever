import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Consistency
import org.slf4j.LoggerFactory
import uk.gov.dwp.dataworks.logging.DataworksLogger

open class HbaseManager { 
    private val logger: DataworksLogger = DataworksLogger(LoggerFactory.getLogger(HbaseManager::class.java))

    open fun getDataFromHbase(tableName: String, formattedKey: ByteArray, family: ByteArray, column: ByteArray, timestamp: Long): ByteArray? {
        hbaseConnection().use  { connection ->
            with (TableName.valueOf(tableName)) {
                if (connection.admin.tableExists(this)) {
                    connection.getTable(this).use { table ->
                        val result = table.get(Get(formattedKey).apply {
                            if (timestamp > 0) {
                                setTimeStamp(timestamp)
                            }
                            addColumn(family, column)
                            setConsistency(Consistency.TIMELINE)
                        })
                        return result.getValue(family, column)
                    }
                }
                else {
                    logger.error("Table does not exist", "hbase_table_name" to tableName)
                    return null
                }
            }
        }
    }

    open fun deleteMessagesFromTopic(dataFamily: ByteArray, dataQualifier: ByteArray, tableName: String, deleteEntireTable: Boolean): ByteArray? {
        hbaseConnection().use { connection ->
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
                with (TableName.valueOf(tableName)) {
                    if (connection.admin.tableExists(this)) {
                        connection.admin.disableTable(this)
                        connection.admin.truncateTable(this, false)
                    }
                }
            }
        }
        return dataQualifier
    }

    open fun hbaseConnection(): Connection = ConnectionFactory.createConnection(HBaseConfiguration.create(HbaseConfig.config))
}
