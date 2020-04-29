import org.apache.hadoop.conf.Configuration

object HbaseConfig {
    val config = Configuration().apply {
        // See also https://hbase.apache.org/book.html#hbase_default_configurations
        set("zookeeper.znode.parent", getEnv("ZOOKEEPER_PARENT") ?: "/hbase")
        set("hbase.zookeeper.quorum", getEnv("ZOOKEEPER_QUORUM") ?: "zookeeper")
        setInt("hbase.zookeeper.port", getEnv("ZOOKEEPER_PORT")?.toIntOrNull() ?: 2181)
        setInt("hbase.client.scanner.timeout.period", getEnv("HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD").toIntOrNull() ?: 60000)
        setInt("hbase.client.scanner.caching", getEnv("HBASE_CLIENT_SCANNER_SCANNER_CACHING").toIntOrNull() ?: 1)
    }

    val dataFamily = getEnv("FAMILY") ?: "cf"
    val dataColumn = getEnv("COLUMN") ?: "record"

    private fun getEnv(envVar: String): String? {
        val value = System.getenv(envVar)
        return if (value.isNullOrEmpty()) null else value
    }
}