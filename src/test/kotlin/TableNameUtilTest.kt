package app.utils

import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import org.junit.Test

class TableNameUtilTest {

    @Test
    fun ensureAllCharactersIsAValidTopicName() {
        assertValidTopicNameIsAMatch("db.", "database","collection")
    }

    @Test
    fun ensureAlphaNumericIsAValidTopicName() {
        assertValidTopicNameIsAMatch("db.", "database1","collection1")
    }

    @Test
    fun ensureAlphanumericWithHyphensIsAValidTopicName() {
        assertValidTopicNameIsAMatch("db.", "database-1","collection-1")
    }

    @Test
    fun ensureAlphamunericWithUnderscoresIsAValidTopicName() {
        assertValidTopicNameIsAMatch("db.", "database_1","collection_1")
    }

    @Test
    fun ensureNoPrefixIsAValidTopicName() {
        assertValidTopicNameIsAMatch("", "database_1","collection_1")
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

    private fun assertValidTopicNameIsAMatch(prefix: String, database: String, collection: String) {
        val allChars = "$database.$collection"
        if (prefix != "") {
            allChars = "$prefix$allChars"
        }
        
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
