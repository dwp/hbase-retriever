package app.utils

import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
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
    fun ensureParsedTableNameReturnedWithValidTopicName() {
        val actualTableName = TableNameUtil().getQualifiedTableName("db.database-1.collection-1")
        actualTableName shouldBe "database_1:collection_1"
    }

    @Test
    fun ensureExceptionIsThrownWithInvalidTopicName() {
        val exception = shouldThrow<Exception> {
            TableNameUtil().getQualifiedTableName("database.collection")
        }
        exception.message shouldBe "Could not parse table name from topic: 'database.collection'"
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
}
