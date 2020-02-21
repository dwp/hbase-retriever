package app.utils

import org.junit.Assert.*
import org.junit.Test

class TableNameUtilTest {

    @Test
    fun ensureAllCharactersIsAValidTopicName() {
        assertValidTopicNameIsAMatch("database","collection")
    }

    @Test
    fun ensureAlphaNumericIsAValidTopicName() {
        assertValidTopicNameIsAMatch("database1","collection1")
    }

    @Test
    fun ensureAlphanumericWithHyphensIsAValidTopicName() {
        assertValidTopicNameIsAMatch("database-1","collection-1")
    }

    @Test
    fun ensureAlphamunericWithUnderscoresIsAValidTopicName() {
        assertValidTopicNameIsAMatch("database_1","collection_1")
    }

    @Test
    fun ensureNonAlphanumericHyphenOrUnderscoreCharacterInDatabaseIsNotAValidTopicName() {
        assertInvalidTopicNameIsNotAMatch("database_1 ","collection_1")
    }

    @Test
    fun ensureNonAlphanumericHyphenOrUnderscoreCharacterInCollectionIsNotAValidTopicName() {
        assertInvalidTopicNameIsNotAMatch("database_1 ","collection_1!")
    }

    @Test
    fun ensureDefaultTableNameReturnedWhenNotUsingTablePerTopicOptionWithValidTopicName() {
        assertCorrectTableNameIsReturned("db.database.collection", false, HbaseConfig.dataTable)
    }

    @Test
    fun ensureDefaultTableNameReturnedWhenNotUsingTablePerTopicOptionWithInvalidTopicName() {
        assertCorrectTableNameIsReturned("db collection", false, HbaseConfig.dataTable)
    }

    @Test
    fun ensureDefaultTableNameReturnedWhenUsingTablePerTopicOptionWithInvalidTopicName() {
        assertCorrectTableNameIsReturned("database.collection", true, HbaseConfig.dataTable)
    }

    @Test
    fun ensureParsedTableNameReturnedWhenUsingTablePerTopicOptionWithValidTopicName() {
        assertCorrectTableNameIsReturned("db.database-1.collection-1", true, "database_1:collection_1")
    }

    private fun assertValidTopicNameIsAMatch(database: String, collection: String) {
        val allChars = "ab.$database.$collection"
        val matcher = TableNameUtil().topicNameTableMatcher(allChars)
        assertNotNull(matcher)
        if (matcher != null) {
            val actualDatabase = matcher.groupValues[1]
            val actualCollection = matcher.groupValues[2]
            assertEquals(database, actualDatabase)
            assertEquals(collection, actualCollection)
        }
    }

    private fun assertInvalidTopicNameIsNotAMatch(database: String, collection: String) {
        val allChars = "ab.$database.$collection"
        val matcher = TableNameUtil().topicNameTableMatcher(allChars)
        assertNull(matcher)
    }

    private fun assertCorrectTableNameIsReturned(topic: String, useTablePerTopic: Boolean, expectedTableName: String) {
        val tableName = TableNameUtil().getQualifiedTableName(topic, useTablePerTopic)
        assertEquals(expectedTableName, tableName)
    }
}