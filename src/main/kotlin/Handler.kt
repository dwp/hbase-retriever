import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout

@Suppress("unused")
class Handler : RequestHandler<Request, ByteArray?> {
    init {
        val consoleAppender = ConsoleAppender()
        consoleAppender.layout = PatternLayout("%d [%p|%c|%C{1}] %m%n")
        consoleAppender.threshold = Level.INFO
        consoleAppender.activateOptions()
        Logger.getRootLogger().addAppender(consoleAppender)
    }

    val logger = Logger.getLogger("Handler")

    override fun handleRequest(input: Request, context: Context?): ByteArray? {
        val topic = input.topic.toByteArray()
        val key = input.key.toByteArray()
        val timestamp = input.timestamp
        val family = HbaseConfig.family.toByteArray()

        logger.info("Fetching data for topic $topic with key $key and timestamp $timestamp for family $family")

        // Connect to Hbase using configured values
        ConnectionFactory.createConnection(HBaseConfiguration.create(HbaseConfig.config)).use { connection ->
            connection.getTable(TableName.valueOf(HbaseConfig.table)).use { table ->
                val result = table.get(Get(key).apply {
                    if (input.timestamp > 0) {
                        setTimeStamp(timestamp)
                    }

                    addColumn(family, topic)
                })

                // Return the value of the cell directly, parsed as a UTF-8 string
                return result.getValue(family, topic)
            }
        }
    }
}
