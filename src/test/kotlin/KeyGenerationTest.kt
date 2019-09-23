import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.properties.assertAll
import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.specs.StringSpec
import com.beust.klaxon.JsonObject
import java.util.*


class KeyGenerationTest : StringSpec({
    val keyGeneration = KeyGeneration()

    "valid input converts to json" {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = keyGeneration.convertToJson(jsonString.toByteArray())

        json should beInstanceOf<JsonObject>()
        json.string("testOne") shouldBe "test1"
        json.int("testTwo") shouldBe 2
    }

    "valid nested input converts to json" {
        val jsonString = "{\"testOne\":{\"testTwo\":2}}"
        val json: JsonObject = keyGeneration.convertToJson(jsonString.toByteArray())
        val jsonTwo: JsonObject = json.obj("testOne") as JsonObject

        json should beInstanceOf<JsonObject>()
        jsonTwo.int("testTwo") shouldBe 2
    }

    "invalid nested input throws exception" {
        val jsonString = "{\"testOne\":"

        val exception = shouldThrow<IllegalArgumentException> {
            keyGeneration.convertToJson(jsonString.toByteArray())
        }
        exception.message shouldBe "Cannot parse invalid JSON"
    }

    "sorts json by key name" {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testB\":true}"
        val jsonObjectUnsorted: JsonObject = keyGeneration.convertToJson(jsonStringUnsorted.toByteArray())
        val jsonStringSorted = "{\"testA\":\"test1\",\"testB\":true,\"testC\":2}"

        val sortedJson = keyGeneration.sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    "sorts json by key name case sensitively" {
        val jsonStringUnsorted = "{\"testb\":true, \"testA\":\"test1\", \"testC\":2}"
        val jsonObjectUnsorted: JsonObject = keyGeneration.convertToJson(jsonStringUnsorted.toByteArray())
        val jsonStringSorted = "{\"testA\":\"test1\",\"testC\":2,\"testb\":true}"

        val sortedJson = keyGeneration.sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    "checksums are different with different inputs" {
        val jsonStringOne = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val jsonStringTwo = "{\"testOne\":\"test2\", \"testTwo\":2}"
        val checksum = keyGeneration.generateFourByteChecksum(jsonStringOne)
        val checksumTwo = keyGeneration.generateFourByteChecksum(jsonStringTwo)

        checksum shouldNotBe checksumTwo
    }

    "can generate consistent checksums from json" {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = keyGeneration.convertToJson(jsonString.toByteArray())
        val checksumOne = keyGeneration.generateFourByteChecksum(json.toString())
        val checksumTwo = keyGeneration.generateFourByteChecksum(json.toString())

        checksumOne shouldBe checksumTwo
    }

    "generated checksums are four bytes" {
        assertAll { input: String ->
            val checksum = keyGeneration.generateFourByteChecksum(input)
            checksum.size shouldBe 4
        }
    }

    "generated keys are consistent for identical inputs" {
        val json = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(json)
        val keyTwo: ByteArray = keyGeneration.generateKey(json)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    "generated keys are different for different inputs" {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":3}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "generated keys are consistent for identical inputs regardless of order" {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testTwo\":2, \"testOne\":\"test1\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    "generated keys are consistent for identical inputs regardless of whitespace" {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{    \"testOne\":              \"test1\",            \"testTwo\":  2}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    "generated keys are consistent for identical inputs regardless of order and whitespace" {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{    \"testTwo\":              2,            \"testOne\":  \"test1\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    "generated keys will vary given values with different whitespace" {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test 1\", \"testTwo\":2}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "generated keys will vary given values that are string and int in each input" {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"2\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "generated keys will vary given values that are string and float in each input" {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":2.0}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"2.0\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "generated keys will vary given values that are string and boolean in each input" {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":false}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"false\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "generated keys will vary given values that are string and null in each input" {
        val jsonOne = "{\"testOne\":\"test1\", \"testTwo\":null}".toByteArray()
        val jsonTwo = "{\"testOne\":\"test1\", \"testTwo\":\"null\"}".toByteArray()
        
        val keyOne: ByteArray = keyGeneration.generateKey(jsonOne)
        val keyTwo: ByteArray = keyGeneration.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }
})
