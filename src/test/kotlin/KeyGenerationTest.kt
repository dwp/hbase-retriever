import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.properties.assertAll
import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.specs.StringSpec
import com.beust.klaxon.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThat
import org.junit.Test
import org.hamcrest.CoreMatchers.instanceOf;


class KeyGenerationTest {
    val keyGeneration = KeyGeneration()

    @Test
    fun printableKeyShouldRenderSuccessfully() {
        val jsonString = "{\"testOne\":\"test1\",\n\"testTwo\":2}"
        val key = keyGeneration.generateKey(jsonString.toByteArray())
        val printable = keyGeneration.printableKey(key)
        assertEquals(printable, """\xfb\xdb\xd9\xb1{"testOne":"test1","testTwo":2}""")
    }

    @Test
    fun validInputConvertsToJson() {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = keyGeneration.convertToJson(jsonString.toByteArray())

        assertThat(json, instanceOf(JsonObject::class.java))
        assertEquals(json.string("testOne"), "test1")
        assertEquals(json.int("testTwo"), 2)
    }

    @Test
    fun validNestedInputConvertsToJson() {
        val jsonString = "{\"testOne\":{\"testTwo\":2}}"
        val json: JsonObject = keyGeneration.convertToJson(jsonString.toByteArray())
        val jsonTwo: JsonObject = json.obj("testOne") as JsonObject

        assertThat(json, instanceOf(JsonObject::class.java))
        assertEquals(jsonTwo.int("testTwo"), 2)
    }

    @Test
    fun validStringInputConvertsToJson() {
        val jsonString = "afa10ffa-ce12-4a39-8db8-b9af1be09e2a"
        val json: JsonObject = keyGeneration.convertToJson(jsonString.toByteArray())
        val jsonTwo: JsonObject = json.obj("id") as JsonObject

        assertThat(json, instanceOf(JsonObject::class.java))
        assertEquals(jsonTwo["id"], jsonString)
    }

    @Test
    fun invalidStringInputThrowsException() {
        val jsonString = "NotAGuid"

        val exception = shouldThrow<IllegalArgumentException> {
            keyGeneration.convertToJson(jsonString.toByteArray())
        }
        assertEquals(exception.message, "Cannot parse invalid JSON")
    }

    @Test
    fun invalidNestedInputThrowsException() {
        val jsonString = "{\"testOne\":"

        val exception = shouldThrow<IllegalArgumentException> {
            keyGeneration.convertToJson(jsonString.toByteArray())
        }
        assertEquals(exception.message, "Cannot parse invalid JSON")
    }

    @Test
    fun sortsJsonByKeyName() {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testB\":true}"
        val jsonObjectUnsorted: JsonObject = keyGeneration.convertToJson(jsonStringUnsorted.toByteArray())
        val jsonStringSorted = "{\"testA\":\"test1\",\"testB\":true,\"testC\":2}"

        val sortedJson = keyGeneration.sortJsonByKey(jsonObjectUnsorted)

        assertEquals(sortedJson, jsonStringSorted)
    }

    @Test
    fun sortsJsonByKeyNameCaseSensitively() {
        val jsonStringUnsorted = "{\"testb\":true, \"testA\":\"test1\", \"testC\":2}"
        val jsonObjectUnsorted: JsonObject = keyGeneration.convertToJson(jsonStringUnsorted.toByteArray())
        val jsonStringSorted = "{\"testA\":\"test1\",\"testC\":2,\"testb\":true}"

        val sortedJson = keyGeneration.sortJsonByKey(jsonObjectUnsorted)

        assertEquals(sortedJson, jsonStringSorted)
    }

    @Test
    fun checksumsAreDifferentWithDifferentInputs() {
        val jsonStringOne = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val jsonStringTwo = "{\"testOne\":\"test2\", \"testTwo\":2}"
        val checksum = keyGeneration.generateFourByteChecksum(jsonStringOne)
        val checksumTwo = keyGeneration.generateFourByteChecksum(jsonStringTwo)

        assertNotEquals(checksum, checksumTwo)
    }

    @Test
    fun canGenerateConsistentChecksumsFromJson() {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = keyGeneration.convertToJson(jsonString.toByteArray())
        val checksumOne = keyGeneration.generateFourByteChecksum(json.toString())
        val checksumTwo = keyGeneration.generateFourByteChecksum(json.toString())

        assertEquals(checksumOne, checksumTwo)
    }

    @Test
    fun canGenerateConsistentChecksumsFromGuidInputs() {
        val guidString = "ff468955-9cf2-4047-a105-e5e7ae6f5b99"
        val json: JsonObject = keyGeneration.convertToJson(guidString.toByteArray())
        val idObject = JsonObject()
        idObject["id"] = String(guidString.toByteArray())
        val checksumOne = keyGeneration.generateFourByteChecksum(json.toString())
        val checksumTwo = keyGeneration.generateFourByteChecksum(json.toString())

        assertEquals(checksumOne, checksumTwo)
    }

    @Test
    fun canGenerateConsistentChecksumsFromGuidInputAndConvertedInputs() {
        val guidString = "ff468955-9cf2-4047-a105-e5e7ae6f5b99"
        val json: JsonObject = keyGeneration.convertToJson(guidString.toByteArray())
        val idObject = JsonObject()
        idObject["id"] = String(guidString.toByteArray())
        val checksumOne = keyGeneration.generateFourByteChecksum(json.toString())
        val checksumTwo = keyGeneration.generateFourByteChecksum(idObject.toString())

        assertEquals(checksumOne, checksumTwo)
    }

    @Test
    fun generatedChecksumsAreFourBytes() {
        assertAll { input: String ->
            val checksum = keyGeneration.generateFourByteChecksum(input)
            assertEquals(checksum.size, 4)
        }
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputs() {
        val json = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(json)
        val keyTwo: ByteArray = keyGeneration.generateKey(json)

        assertTrue(keyOne.contentEquals(keyTwo))
    }

    @Test
    fun generatedKeysAreDifferentForDifferentInputs() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":3}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        assertFalse(keyOne.contentEquals(keyTwo))
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputsRegardlessOfOrder() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testTwo\":2, \"testOne\":\"test1\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        assertTrue(keyOne.contentEquals(keyTwo))
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputsRegardlessOfWhitespace() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{    \"testOne\":              \"test1\",            \"testTwo\":  2}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        assertTrue(keyOne.contentEquals(keyTwo))
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputsRegardlessOfOrderAndWhitespace() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{    \"testTwo\":              2,            \"testOne\":  \"test1\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        assertTrue(keyOne.contentEquals(keyTwo))
    }

    @Test
    fun generatedKeysWillVaryGivenValuesWithDifferentWhitespace() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test 1\", \"testTwo\":2}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        assertFalse(keyOne.contentEquals(keyTwo))
    }

    @Test
    fun generatedKeysWillVaryGivenValuesThatAreStringAndIntInEachInput() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"2\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        assertFalse(keyOne.contentEquals(keyTwo))
    }

    @Test
    fun generatedKeysWillVaryGivenValuesThatAreStringAndFloatInEachInput() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2.0}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"2.0\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        assertFalse(keyOne.contentEquals(keyTwo))
    }

    @Test
    fun generatedKeysWillVaryGivenValuesThatAreStringAndBooleanInEachInput() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":false}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"false\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        assertFalse(keyOne.contentEquals(keyTwo))
    }

    @Test
    fun generatedKeysWillVaryGivenValuesThatAreStringAndNullInEachInput() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":null}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"null\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        assertFalse(keyOne.contentEquals(keyTwo))
    }
}
