import org.apache.hadoop.conf.Configuration

object HbaseConfig {
    val config = Configuration().apply {
        set("hbase.zookeeper.quorum", getEnv("ZOOKEEPER_QUORUM") ?: "zookeeper")
        setInt("hbase.zookeeper.port", getEnv("ZOOKEEPER_PORT")?.toIntOrNull() ?: 2181)
    }

    val table = getEnv("TABLE") ?: "k2hb:ingest"
    val family = getEnv("FAMILY") ?: "topic"

    private fun getEnv(envVar: String): String? {
        val value = System.getenv(envVar)
        return if (value.isNullOrEmpty()) null else value
    }
}