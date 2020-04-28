import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.properties.assertAll
import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.specs.StringSpec
import com.beust.klaxon.JsonObject
import org.junit.Test
import com.nhaarman.mockitokotlin2.*
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Result


class HbaseManagerTest {
    private val rowKey = "testRowKey".toByteArray()
    private val dataQualifier = "testQualifier".toByteArray()
    private val dataFamily = "testFamily".toByteArray()
    private val dataColumn = "testColumn".toByteArray()
    private val expectedCellValue = "testValue".toByteArray()
    private val validTopic = "db.test.topic"
    private val inputKey = "testKey"
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

        with (HbaseManager()) {
            deleteMessagesFromTopic(connection, dataFamily, dataQualifier, qualifiedTableName, false)
        }

        verify(adm, times(1)).truncateTable(TableName.valueOf(qualifiedTableName), false)
        verify(adm, times(0)).disableTable(TableName.valueOf(qualifiedTableName))
        verify(adm, times(0)).deleteTable(TableName.valueOf(qualifiedTableName))
        verify(connection, times(0)).getTable(any<TableName>())
    }
    
    @Test
    fun deleteHbaseTableWhenRequested() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn true
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        with (HbaseManager()) {
            deleteMessagesFromTopic(connection, dataFamily, dataQualifier, qualifiedTableName, true)
        }

        verify(adm, times(1)).disableTable(TableName.valueOf(qualifiedTableName))
        verify(adm, times(1)).deleteTable(TableName.valueOf(qualifiedTableName))
        verify(adm, times(0)).truncateTable(TableName.valueOf(qualifiedTableName), false)
        verify(connection, times(0)).getTable(any<TableName>())
    }
    
    @Test
    fun getCorrectValueWhenTimestampIsUsed() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn true
        }

        val result = mock<Result>() {
            on { getValue(dataFamily, dataColumn) } doReturn expectedCellValue 
        }

        val getWithValues = Get(rowKey)
            .setTimeStamp(timestamp)
            .addColumn(dataFamily, dataColumn)

        val table = mock<Table>() {
            on { get(getWithValues) } doReturn result
        }
        
        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        with (HbaseManager()) {
            val actualCellValue = getDataFromHbase(connection, qualifiedTableName, rowKey, dataFamily, dataColumn, timestamp)

            verify(connection, times(1)).getTable(TableName.valueOf(qualifiedTableName))
            verify(table, times(1)).get(getWithValues)
            verify(result, times(1)).getValue(dataFamily, dataColumn)

            actualCellValue shouldBe expectedCellValue
        }
    }
    
    @Test
    fun getCorrectValueWhenNoTimestampIsUsed() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn true
        }

        val result = mock<Result>() {
            on { getValue(dataFamily, dataColumn) } doReturn expectedCellValue 
        }

        val getWithValues = Get(rowKey)
            .addColumn(dataFamily, dataColumn)

        val table = mock<Table>() {
            on { get(getWithValues) } doReturn result
        }
        
        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        with (HbaseManager()) {
            val actualCellValue = getDataFromHbase(connection, qualifiedTableName, rowKey, dataFamily, dataColumn, 0L)

            verify(connection, times(1)).getTable(TableName.valueOf(qualifiedTableName))
            verify(table, times(1)).get(getWithValues)
            verify(result, times(1)).getValue(dataFamily, dataColumn)

            actualCellValue shouldBe expectedCellValue
        }
    }
    
    @Test
    fun getNullValueWhenTableDoesNotExist() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn false
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        with (HbaseManager()) {
            val actualCellValue = getDataFromHbase(connection, qualifiedTableName, rowKey, dataFamily, dataColumn, 0L)
            actualCellValue shouldBe null
        }
    }
}
