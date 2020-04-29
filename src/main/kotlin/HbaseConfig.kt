import org.apache.hadoop.conf.Configuration

object HbaseConfig {
    val config = Configuration().apply {
        // See also https://hbase.apache.org/book.html#hbase_default_configurations
        set("zookeeper.znode.parent", getEnv("ZOOKEEPER_PARENT") ?: "/hbase")
        set("hbase.zookeeper.quorum", getEnv("ZOOKEEPER_QUORUM") ?: "zookeeper")
        setInt("hbase.zookeeper.port", getEnv("ZOOKEEPER_PORT")?.toIntOrNull() ?: 2181)
    }

    val dataFamily = getEnv("FAMILY") ?: "cf"
    val dataColumn = getEnv("COLUMN") ?: "record"

    private fun getEnv(envVar: String): String? {
        val value = System.getenv(envVar)
        return if (value.isNullOrEmpty()) null else value
    }
}