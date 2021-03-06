package app.utils

import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException
import com.beust.klaxon.Parser
import org.slf4j.LoggerFactory
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.nio.ByteBuffer
import java.util.zip.CRC32

open class KeyGenerationUtil {
    private val logger: DataworksLogger = DataworksLogger(LoggerFactory.getLogger(KeyGenerationUtil::class.java))
    private val alphanumericAndHyphensRegex = Regex("^[0-9a-fA-F-]*$")

    open fun generateKey(jsonString: ByteArray): ByteArray {
        val json = convertToJson(jsonString)
        val jsonOrdered = sortJsonByKey(json)
        val checksumBytes: ByteArray = generateFourByteChecksum(jsonOrdered)
        
        return checksumBytes.plus(jsonOrdered.toByteArray())
    }

    fun convertToJson(body: ByteArray): JsonObject {
        val stringBuilder: StringBuilder = StringBuilder(String(body))
        return getId(stringBuilder)
    }

    fun getId(bodyString: StringBuilder): JsonObject {
        if (alphanumericAndHyphensRegex.matches(String(bodyString))) {
            val idObject = JsonObject()
            idObject["id"] = String(bodyString)
            logger.info("Incoming id string matched GUID and has been changed",
                "original_id" to String(bodyString),
                "transformed_id" to idObject.toString())
            return idObject
        }

        try {
            val parser: Parser = Parser.default()
            val stringBuilder = StringBuilder(bodyString)
            return parser.parse(stringBuilder) as JsonObject
        } catch (e: KlaxonException) {
            logger.error(
                "Error while parsing message body in to json", e,
                "message_body" to String(bodyString))
            throw IllegalArgumentException("Cannot parse invalid JSON")
        }
    }

    fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()

        checksum.update(bytes, 0, bytes.size)

        return ByteBuffer.allocate(4).putInt(checksum.value.toInt()).array()
    }

    fun sortJsonByKey(unsortedJson: JsonObject): String {
        val sortedEntries = unsortedJson.toSortedMap(compareBy { it })
        val json: JsonObject = JsonObject(sortedEntries)
        return json.toJsonString()
    }

    open fun printableKey(key: ByteArray): String {
        val hash = key.slice(IntRange(0, 3))
        val hex = hash.map { String.format("\\x%02x", it) }.joinToString("")
        val renderable = key.slice(IntRange(4, key.size - 1)).map{ it.toChar() }.joinToString("")
        return "${hex}${renderable}"
    }
}
