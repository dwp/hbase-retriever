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
import app.utils.TableNameUtil
import app.utils.KeyGenerationUtil

class HandlerTest {
    private val expectedDataQualifier = "testQualifier".toByteArray()
    private val dataFamily = "testFamily"
    private val dataColumn = "testColumn"
    private val expectedCellValue = "testValue".toByteArray()
    private val validTopic = "db.test.topic"
    private val inputKey = "testKey"
    private val formattedKey = "testKeyFormatted".toByteArray()
    private val printableKey = "testKeyFormatted"
    private val qualifiedTableName = "test_topic"
    private val timestamp = 1L
    
    @Test
    fun canProcessValidGetRequest() {
        val deleteRequestValue = false
        val deleteEntireTableWhenInDeleteModeValue = false

        val connection = mock<Connection>()

        val keyGeneration = mock<KeyGenerationUtil>() {
            on { generateKey(inputKey.toByteArray()) } doReturn formattedKey
            on { printableKey(formattedKey) } doReturn printableKey
        }

        val tableNameUtil = mock<TableNameUtil>() {
            on { getQualifiedTableName(validTopic) } doReturn qualifiedTableName 
        }

        val hbaseManager = mock<HbaseManager>() {
            on { deleteMessagesFromTopic(connection, dataFamily.toByteArray(), dataColumn.toByteArray(), qualifiedTableName, deleteEntireTableWhenInDeleteModeValue) } doReturn expectedDataQualifier 
            on { getDataFromHbase(connection, qualifiedTableName, formattedKey, dataFamily.toByteArray(), dataColumn.toByteArray(), timestamp) } doReturn expectedCellValue 
        }

        val request = Request()
        request.topic = validTopic
        request.key = inputKey
        request.timestamp = timestamp
        request.deleteRequest = deleteRequestValue
        request.deleteEntireTableWhenInDeleteMode = deleteEntireTableWhenInDeleteModeValue

        with (Handler()) {
            val actualCellValue = processRequest(connection, request, keyGeneration, tableNameUtil, hbaseManager, dataFamily, dataColumn)

            verify(tableNameUtil, times(1)).getQualifiedTableName(validTopic)
            verify(keyGeneration, times(1)).generateKey(inputKey.toByteArray())
            verify(keyGeneration, times(1)).printableKey(formattedKey)
            verify(hbaseManager, times(1)).getDataFromHbase(connection, qualifiedTableName, formattedKey, dataFamily.toByteArray(), dataColumn.toByteArray(), timestamp)

            verify(hbaseManager, times(0)).deleteMessagesFromTopic(any(), any(), any(), any(), any())

            actualCellValue shouldBe expectedCellValue
        }
    }
    
    @Test
    fun canProcessValidDeleteRequest() {
        val deleteRequestValue = true
        val deleteEntireTableWhenInDeleteModeValue = true

        val connection = mock<Connection>()

        val keyGeneration = mock<KeyGenerationUtil>() {
            on { generateKey(inputKey.toByteArray()) } doReturn formattedKey
            on { printableKey(formattedKey) } doReturn printableKey
        }

        val tableNameUtil = mock<TableNameUtil>() {
            on { getQualifiedTableName(validTopic) } doReturn qualifiedTableName 
        }

        val hbaseManager = mock<HbaseManager>() {
            on { deleteMessagesFromTopic(connection, dataFamily.toByteArray(), dataColumn.toByteArray(), qualifiedTableName, deleteEntireTableWhenInDeleteModeValue) } doReturn expectedDataQualifier 
            on { getDataFromHbase(connection, qualifiedTableName, formattedKey, dataFamily.toByteArray(), dataColumn.toByteArray(), timestamp) } doReturn expectedCellValue 
        }

        val request = Request()
        request.topic = validTopic
        request.key = inputKey
        request.timestamp = timestamp
        request.deleteRequest = deleteRequestValue
        request.deleteEntireTableWhenInDeleteMode = deleteEntireTableWhenInDeleteModeValue

        with (Handler()) {
            val actualDataQualifier = processRequest(connection, request, keyGeneration, tableNameUtil, hbaseManager, dataFamily, dataColumn)

            verify(tableNameUtil, times(1)).getQualifiedTableName(validTopic)
            verify(hbaseManager, times(1)).deleteMessagesFromTopic(connection, dataFamily.toByteArray(), dataColumn.toByteArray(), qualifiedTableName, deleteEntireTableWhenInDeleteModeValue)

            verify(keyGeneration, times(0)).generateKey(any())
            verify(keyGeneration, times(0)).printableKey(any())
            verify(hbaseManager, times(0)).getDataFromHbase(any(), any(), any(), any(), any(), any())

            actualDataQualifier shouldBe expectedDataQualifier
        }
    }
    
    @Test
    fun canProcessValidTruncateRequest() {
        val deleteRequestValue = true
        val deleteEntireTableWhenInDeleteModeValue = false

        val connection = mock<Connection>()

        val keyGeneration = mock<KeyGenerationUtil>() {
            on { generateKey(inputKey.toByteArray()) } doReturn formattedKey
            on { printableKey(formattedKey) } doReturn printableKey
        }

        val tableNameUtil = mock<TableNameUtil>() {
            on { getQualifiedTableName(validTopic) } doReturn qualifiedTableName 
        }

        val hbaseManager = mock<HbaseManager>() {
            on { deleteMessagesFromTopic(connection, dataFamily.toByteArray(), dataColumn.toByteArray(), qualifiedTableName, deleteEntireTableWhenInDeleteModeValue) } doReturn expectedDataQualifier 
            on { getDataFromHbase(connection, qualifiedTableName, formattedKey, dataFamily.toByteArray(), dataColumn.toByteArray(), timestamp) } doReturn expectedCellValue 
        }

        val request = Request()
        request.topic = validTopic
        request.key = inputKey
        request.timestamp = timestamp
        request.deleteRequest = deleteRequestValue
        request.deleteEntireTableWhenInDeleteMode = deleteEntireTableWhenInDeleteModeValue

        with (Handler()) {
            val actualDataQualifier = processRequest(connection, request, keyGeneration, tableNameUtil, hbaseManager, dataFamily, dataColumn)

            verify(tableNameUtil, times(1)).getQualifiedTableName(validTopic)
            verify(hbaseManager, times(1)).deleteMessagesFromTopic(connection, dataFamily.toByteArray(), dataColumn.toByteArray(), qualifiedTableName, deleteEntireTableWhenInDeleteModeValue)

            verify(keyGeneration, times(0)).generateKey(any())
            verify(keyGeneration, times(0)).printableKey(any())
            verify(hbaseManager, times(0)).getDataFromHbase(any(), any(), any(), any(), any(), any())

            actualDataQualifier shouldBe expectedDataQualifier
        }
    }
}
