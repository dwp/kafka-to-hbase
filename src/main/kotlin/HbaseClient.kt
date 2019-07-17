import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Put

class HbaseClient(
    private val connection: Connection,
    private val namespace: String,
    private val family: ByteArray,
    private val column: ByteArray
) {
    fun putVersion(topic: ByteArray, key: ByteArray, body: ByteArray, version: Long) {
        val table = connection.getTable(TableName.valueOf(namespace.toByteArray(), topic))
        table.put(Put(key).apply {
            this.addColumn(
                family,
                column,
                version,
                body
            )
        })
    }

    fun close() = connection.close()
}