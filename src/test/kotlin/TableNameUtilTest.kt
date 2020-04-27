package app.utils

import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
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
        matcher shouldNotBe null
        if (matcher != null) {
            val actualDatabase = matcher.groupValues[1]
            val actualCollection = matcher.groupValues[2]
            database shouldBe actualDatabase
            collection shouldBe actualCollection
        }
    }

    private fun assertInvalidTopicNameIsNotAMatch(database: String, collection: String) {
        val allChars = "ab.$database.$collection"
        val matcher = TableNameUtil().topicNameTableMatcher(allChars)
        matcher shouldBe null
    }

    private fun assertCorrectTableNameIsReturned(topic: String, useTablePerTopic: Boolean, expectedTableName: String) {
        val tableName = TableNameUtil().getQualifiedTableName(topic, useTablePerTopic)
        expectedTableName shouldBe tableName
    }
}