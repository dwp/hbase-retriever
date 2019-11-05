import java.util.logging.Logger
import java.util.zip.CRC32
import java.nio.ByteBuffer
import com.beust.klaxon.Parser
import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException

class KeyGeneration {
    private val log: Logger = Logger.getLogger("KeyGeneration")

    fun generateKey(jsonString: ByteArray): ByteArray {
        val json = convertToJson(jsonString)
        val jsonOrdered = sortJsonByKey(json)
        val checksumBytes: ByteArray = generateFourByteChecksum(jsonOrdered)
        
        return checksumBytes.plus(jsonOrdered.toByteArray())
    }

    fun convertToJson(body: ByteArray): JsonObject {

        try {
            val parser: Parser = Parser.default()
            val stringBuilder: StringBuilder = StringBuilder(String(body))
            return parser.parse(stringBuilder) as JsonObject
        } catch (e: KlaxonException) {
            log.warning(
                "Error while parsing message body of '%s' in to json: %s".format(
                    String(body),
                    e.toString()
                )
            )
            throw IllegalArgumentException("Cannot parse invalid JSON")
        }
    }

    fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()

        checksum.update(bytes, 0, bytes.size)

        return ByteBuffer.allocate(4).putInt(checksum.getValue().toInt()).array()
    }

    fun sortJsonByKey(unsortedJson: JsonObject): String {
        val sortedEntries = unsortedJson.toSortedMap(compareBy { it })
        val json = JsonObject(sortedEntries)
        
        return json.toJsonString()
    }

    fun printableKey(key: ByteArray): String {
        val hash = key.slice(IntRange(0, 3))
        val hex = hash.map { String.format("\\x%02x", it) }.joinToString("")
        val renderable = key.slice(IntRange(4, key.size - 1)).map{ it.toChar() }.joinToString("")
        return "${hex}${renderable}"
    }
}