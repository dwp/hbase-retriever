# hbase-retriever

A simple Lambda to allow clients to retrieve values from Hbase in EMR that
have been written by Kafka2Hbase (see https://github.com/dwp/kafka-to-hbase).

To request a record, call the lambda with a document containing a topic and
message key from Kafka. The value of that message as persisted in Hbase
will be returned.

```json
{
  "topic": "db.claimants.address",
  "key": "1c2c47d6-5b8b-4d82-929e-b5b05b1b2306"
}
```

If a specific version (other than "latest") is required, specify the
version as a numeric timestamp.

```json
{
  "topic": "db.claimants.address",
  "key": "1c2c47d6-5b8b-4d82-929e-b5b05b1b2306",
  "timestamp": 1563809926831
}
```

The returned value is the raw bytes as received from Kafka. If the
integration response is set to convert to text, it will be a base64 encoded
string.

## Configuration

Configuration of the Lambda is done entirely via environment variables.

* **ZOOKEEPER_PARENT** - set `hbase.zookeeper.parent` to hbase parent URI (default `/hbase`, aws `/hbase-unsecure`)
* **ZOOKEEPER_QUORUM** - set `hbase.zookeeper.quorum` to a list of hosts (default `zookeeper`)
* **ZOOKEEPER_PORT** - set `hbase.zookeeper.port` to an integer port (default `2181`)
* **TABLE** - the fully qualified Hbase table to read from
* **FAMILY** - the column family to search for

Rows are retrieved from the configured table using the requested key. The
cell value is fetched from the configured column family using the requested
topic as the qualifier. If a timestamp is specified, the matching version is returned.

If no valid row is found, null is returned.