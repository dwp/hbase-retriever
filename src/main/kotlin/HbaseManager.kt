import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Scan
import app.utils.TableNameUtil
import org.slf4j.LoggerFactory
import uk.gov.dwp.dataworks.logging.DataworksLogger

open class HbaseManager { 
    private val logger: DataworksLogger = DataworksLogger(LoggerFactory.getLogger(HbaseManager::class.java))

    open fun getDataFromHbase(connection: Connection, tableName: String, formattedKey: ByteArray, family: ByteArray, column: ByteArray, timestamp: Long): ByteArray? {
        with(connection) {
            with (TableName.valueOf(tableName)) {
                if (connection.admin.tableExists(this)) {
                    connection.getTable(this).use { table ->
                        val result = table.get(Get(formattedKey).apply {
                            if (timestamp > 0) {
                                setTimeStamp(timestamp)
                            }
                            addColumn(family, column)
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

    open fun deleteMessagesFromTopic(connection: Connection, dataFamily: ByteArray, dataQualifier: ByteArray, tableName: String, deleteEntireTable: Boolean): ByteArray? {
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

                with (TableName.valueOf(tableName)) {
                    if (connection.admin.tableExists(this)) {
                        connection.admin.truncateTable(this, false)
                    }
                }
            }
        }
        return dataQualifier
    }
}
