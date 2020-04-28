import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.properties.assertAll
import io.kotlintest.specs.StringSpec
import com.beust.klaxon.JsonObject
import org.junit.Test


class KeyGenerationTest {
    val keyGeneration = KeyGeneration()

    @Test
    fun printableKeyShouldRenderSuccessfully() {
        val jsonString = "{\"testOne\":\"test1\",\n\"testTwo\":2}"
        val key = keyGeneration.generateKey(jsonString.toByteArray())
        val printable = keyGeneration.printableKey(key)
        printable shouldBe """\xfb\xdb\xd9\xb1{"testOne":"test1","testTwo":2}"""
    }

    @Test
    fun validInputConvertsToJson() {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = keyGeneration.convertToJson(jsonString.toByteArray())

        json shouldNotBe null
        json.string("testOne") shouldBe "test1"
        json.int("testTwo") shouldBe 2
    }

    @Test
    fun validNestedInputConvertsToJson() {
        val jsonString = "{\"testOne\":{\"testTwo\":2}}"
        val json: JsonObject = keyGeneration.convertToJson(jsonString.toByteArray())
        val jsonTwo: JsonObject = json.obj("testOne") as JsonObject

        json shouldNotBe null
        jsonTwo.int("testTwo") shouldBe 2
    }

    @Test
    fun validGuidInputConvertsToJson() {
        val guidString = "afa10ffa-ce12-4a39-8db8-b9af1be09e2a"
        val json: JsonObject = keyGeneration.convertToJson(guidString.toByteArray())

        json shouldNotBe null
        json["id"] shouldBe guidString
    }

    @Test
    fun validStringInputConvertsToJson() {
        val jsonString = "b9af1be09e2a"
        val json: JsonObject = keyGeneration.convertToJson(jsonString.toByteArray())

        json shouldNotBe null
        json["id"] shouldBe jsonString
    }

    @Test
    fun invalidStringInputThrowsException() {
        val jsonString = "HasQuotes\""

        val exception = shouldThrow<IllegalArgumentException> {
            keyGeneration.convertToJson(jsonString.toByteArray())
        }

        exception.message shouldBe "Cannot parse invalid JSON"
    }

    @Test
    fun invalidNestedInputThrowsException() {
        val jsonString = "{\"testOne\":"

        val exception = shouldThrow<IllegalArgumentException> {
            keyGeneration.convertToJson(jsonString.toByteArray())
        }

        exception.message shouldBe "Cannot parse invalid JSON"
    }

    @Test
    fun sortsJsonByKeyName() {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testB\":true}"
        val jsonObjectUnsorted: JsonObject = keyGeneration.convertToJson(jsonStringUnsorted.toByteArray())
        val jsonStringSorted = "{\"testA\":\"test1\",\"testB\":true,\"testC\":2}"

        val sortedJson = keyGeneration.sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun sortsJsonByKeyNameCaseSensitively() {
        val jsonStringUnsorted = "{\"testb\":true, \"testA\":\"test1\", \"testC\":2}"
        val jsonObjectUnsorted: JsonObject = keyGeneration.convertToJson(jsonStringUnsorted.toByteArray())
        val jsonStringSorted = "{\"testA\":\"test1\",\"testC\":2,\"testb\":true}"

        val sortedJson = keyGeneration.sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun checksumsAreDifferentWithDifferentInputs() {
        val jsonStringOne = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val jsonStringTwo = "{\"testOne\":\"test2\", \"testTwo\":2}"
        val checksumOne = keyGeneration.generateFourByteChecksum(jsonStringOne)
        val checksumTwo = keyGeneration.generateFourByteChecksum(jsonStringTwo)

        checksumOne shouldNotBe checksumTwo
    }

    @Test
    fun canGenerateConsistentChecksumsFromJson() {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val checksumOne = keyGeneration.generateKey(jsonString.toByteArray())
        val checksumTwo = keyGeneration.generateKey(jsonString.toByteArray())

        checksumOne shouldBe checksumTwo
    }

    @Test
    fun canGenerateConsistentChecksumsFromGuidInputs() {
        val guidString = "ff468955-9cf2-4047-a105-e5e7ae6f5b99"
        val checksumOne = keyGeneration.generateKey(guidString.toByteArray())
        val checksumTwo = keyGeneration.generateKey(guidString.toByteArray())

        checksumOne shouldBe checksumTwo
    }

    @Test
    fun canGenerateConsistentChecksumsFromGuidInputAndConvertedInputs() {
        val guidString = "ff468955-9cf2-4047-a105-e5e7ae6f5b99"
        val json: JsonObject = keyGeneration.convertToJson(guidString.toByteArray())
        val idObject = JsonObject()
        idObject["id"] = String(guidString.toByteArray())
        val checksumOne = keyGeneration.generateFourByteChecksum(json.toString())
        val checksumTwo = keyGeneration.generateFourByteChecksum(idObject.toString())

        checksumOne shouldBe checksumTwo
    }

    @Test
    fun canGenerateConsistentChecksumsFromMongoInputs() {
        val guidString = "5e7ae6f5b99"
        val json: JsonObject = keyGeneration.convertToJson(guidString.toByteArray())
        val idObject = JsonObject()
        idObject["id"] = String(guidString.toByteArray())
        val checksumOne = keyGeneration.generateFourByteChecksum(json.toString())
        val checksumTwo = keyGeneration.generateFourByteChecksum(json.toString())

        checksumOne shouldBe checksumTwo
    }

    @Test
    fun canGenerateConsistentChecksumsFromMongoInputAndConvertedInputs() {
        val guidString = "5e7ae6f5b99"
        val json: JsonObject = keyGeneration.convertToJson(guidString.toByteArray())
        val idObject = JsonObject()
        idObject["id"] = String(guidString.toByteArray())
        val checksumOne = keyGeneration.generateFourByteChecksum(json.toString())
        val checksumTwo = keyGeneration.generateFourByteChecksum(idObject.toString())

        checksumOne shouldBe checksumTwo
    }

    @Test
    fun generatedChecksumsAreFourBytes() {
        assertAll { input: String ->
            val checksum = keyGeneration.generateFourByteChecksum(input)
            checksum.size shouldBe 4
        }
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputs() {
        val json = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(json)
        val keyTwo: ByteArray = keyGeneration.generateKey(json)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generatedKeysAreDifferentForDifferentInputs() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":3}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputsRegardlessOfOrder() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testTwo\":2, \"testOne\":\"test1\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputsRegardlessOfWhitespace() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{    \"testOne\":              \"test1\",            \"testTwo\":  2}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputsRegardlessOfOrderAndWhitespace() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{    \"testTwo\":              2,            \"testOne\":  \"test1\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generatedKeysWillVaryGivenValuesWithDifferentWhitespace() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test 1\", \"testTwo\":2}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generatedKeysWillVaryGivenValuesThatAreStringAndIntInEachInput() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"2\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generatedKeysWillVaryGivenValuesThatAreStringAndFloatInEachInput() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2.0}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"2.0\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generatedKeysWillVaryGivenValuesThatAreStringAndBooleanInEachInput() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":false}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"false\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generatedKeysWillVaryGivenValuesThatAreStringAndNullInEachInput() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":null}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"null\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun getIdReturnsObjectWithEmbeddedGuidForGuidInput() {
        val guidString = "ff468955-9cf2-4047-a105-e5e7ae6f5b99"
        
        val expectedObject = JsonObject()
        expectedObject["id"] = guidString

        val actualObject = keyGeneration.convertToJson(guidString.toByteArray())

        expectedObject shouldBe actualObject
    }

    @Test
    fun getIdReturnsObjectForObjectInput() {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}"
        
        val expectedObject = JsonObject()
        expectedObject["testOne"] = "test1"
        expectedObject["testTwo"] = 2

        val actualObject = keyGeneration.convertToJson(jsonOne.toByteArray())

        expectedObject shouldBe actualObject
    }
}
