
import com.nhaarman.mockitokotlin2.*
import io.kotlintest.shouldBe
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.junit.Test


class HbaseManagerTest {
    private val rowKey = "testRowKey".toByteArray()
    private val dataQualifier = "testQualifier".toByteArray()
    private val dataFamily = "testFamily".toByteArray()
    private val dataColumn = "testColumn".toByteArray()
    private val expectedCellValue = "testValue".toByteArray()
    private val qualifiedTableName = "test_table"
    private val timestamp = 1L

    @Test
    fun truncateHbaseTableWhenRequested() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn true
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        val hbaseManager = spy<HbaseManager> {
            on { hbaseConnection() } doReturn connection
        }

        hbaseManager.deleteMessagesFromTopic(dataFamily, dataQualifier, qualifiedTableName, false)

        verify(adm, times(1)).disableTable(TableName.valueOf(qualifiedTableName))
        verify(adm, times(1)).truncateTable(TableName.valueOf(qualifiedTableName), false)
        verify(adm, times(0)).deleteTable(TableName.valueOf(qualifiedTableName))
        verify(connection, times(0)).getTable(any())
        verify(connection, times(1)).close()
    }
    
    @Test
    fun deleteHbaseTableWhenRequested() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn true
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        val hbaseManager = spy<HbaseManager> {
            on { hbaseConnection() } doReturn connection
        }

        hbaseManager.deleteMessagesFromTopic(dataFamily, dataQualifier, qualifiedTableName, true)

        verify(adm, times(1)).disableTable(TableName.valueOf(qualifiedTableName))
        verify(adm, times(1)).deleteTable(TableName.valueOf(qualifiedTableName))
        verify(adm, times(0)).truncateTable(TableName.valueOf(qualifiedTableName), false)
        verify(connection, times(0)).getTable(any())
        verify(connection, times(1)).close()
    }
    
    @Test
    fun getCorrectValueWhenTimestampIsUsed() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn true
        }

        val result = mock<Result> {
            on { getValue(dataFamily, dataColumn) } doReturn expectedCellValue 
        }

        val getWithValues = Get(rowKey)
            .setTimeStamp(timestamp)
            .addColumn(dataFamily, dataColumn)

        val table = mock<Table> {
            on { get(getWithValues) } doReturn result
        }
        
        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        val hbaseManager = spy<HbaseManager> {
            on { hbaseConnection() } doReturn connection
        }

        val actualCellValue = hbaseManager.getDataFromHbase(qualifiedTableName, rowKey, dataFamily, dataColumn, timestamp)

        verify(connection, times(1)).getTable(TableName.valueOf(qualifiedTableName))
        verify(table, times(1)).get(getWithValues)
        verify(result, times(1)).getValue(dataFamily, dataColumn)
        verify(connection, times(1)).close()

        actualCellValue shouldBe expectedCellValue
    }
    
    @Test
    fun getCorrectValueWhenNoTimestampIsUsed() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn true
        }

        val result = mock<Result> {
            on { getValue(dataFamily, dataColumn) } doReturn expectedCellValue 
        }

        val getWithValues = Get(rowKey)
            .addColumn(dataFamily, dataColumn)

        val table = mock<Table> {
            on { get(getWithValues) } doReturn result
        }
        
        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        val hbaseManager = spy<HbaseManager> {
            on { hbaseConnection() } doReturn connection
        }

        val actualCellValue = hbaseManager.getDataFromHbase(qualifiedTableName, rowKey, dataFamily, dataColumn, 0L)

        verify(connection, times(1)).getTable(TableName.valueOf(qualifiedTableName))
        verify(table, times(1)).get(getWithValues)
        verify(result, times(1)).getValue(dataFamily, dataColumn)
        verify(connection, times(1)).close()

        actualCellValue shouldBe expectedCellValue
    }
    
    @Test
    fun getNullValueWhenTableDoesNotExist() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn false
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        val hbaseManager = spy<HbaseManager> {
            on { hbaseConnection() } doReturn connection
        }

        val actualCellValue = hbaseManager.getDataFromHbase(qualifiedTableName, rowKey, dataFamily, dataColumn, 0L)
        actualCellValue shouldBe null
        verify(connection, times(1)).close()
    }
}
