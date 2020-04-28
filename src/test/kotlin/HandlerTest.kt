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
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Result


class HandlerTest {
    private val rowKey = "testRowKey".toByteArray()
    private val dataQualifier = "testQualifier".toByteArray()
    private val dataFamily = "testFamily".toByteArray()
    private val qualifiedTableName = "test_table"
    
    @Test
    fun clearHbaseTableWhenRequested() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn true
        }

        val result = mock<Result>() {
            on { row } doReturn rowKey 
        }

        val resultList = mutableListOf<Result>()
        resultList.add(result)
        val scanner = mock<ResultScanner>() {
            on { iterator() } doReturn resultList.iterator()
        }

        val deleteList = mutableListOf<Delete>()
        val delete = Delete(rowKey).addColumns(dataFamily, dataQualifier)
        deleteList.add(delete)

        val table = mock<Table>() {
            on { getScanner(any<Scan>()) } doReturn scanner
        }

        doNothing().whenever(table).delete(deleteList)
        
        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        with (Handler()) {
            deleteMessagesFromTopic(connection, dataFamily, dataQualifier, qualifiedTableName, false)
        }

        verify(connection, times(1)).getTable(TableName.valueOf(qualifiedTableName))
        verify(table, times(1)).delete(refEq(deleteList))
    }
    
    @Test
    fun deleteHbaseTableWhenRequested() {
        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(qualifiedTableName)) } doReturn true
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        with (Handler()) {
            deleteMessagesFromTopic(connection, dataFamily, dataQualifier, qualifiedTableName, true)
        }

        verify(adm, times(1)).disableTable(TableName.valueOf(qualifiedTableName))
        verify(adm, times(1)).deleteTable(TableName.valueOf(qualifiedTableName))
        verify(connection, times(0)).getTable(any<TableName>())
    }
}
