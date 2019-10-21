import org.apache.hadoop.conf.Configuration

object HbaseConfig {
    val config = Configuration().apply {
        // See also https://hbase.apache.org/book.html#hbase_default_configurations
        set("zookeeper.znode.parent", getEnv("ZOOKEEPER_PARENT") ?: "/hbase")
        set("hbase.zookeeper.quorum", getEnv("ZOOKEEPER_QUORUM") ?: "zookeeper")
        setInt("hbase.zookeeper.port", getEnv("ZOOKEEPER_PORT")?.toIntOrNull() ?: 2181)
    }

    val dataTable = getEnv("TABLE") ?: "k2hb:ingest"
    val dataFamily = getEnv("FAMILY") ?: "topic"
    val topicTable = getEnv("TOPIC_TABLE") ?: "k2hb:ingest-topic"
    val topicFamily = getEnv("TOPIC_FAMILY") ?: "c"
    val topicQualifier = getEnv("TOPIC_QUALIFIER") ?: "msg"

    private fun getEnv(envVar: String): String? {
        val value = System.getenv(envVar)
        return if (value.isNullOrEmpty()) null else value
    }
}