import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.client.Put

class HbaseClient(
    private val connection: Connection,
    private val dataTable: String,
    private val dataFamily: ByteArray,
    private val topicTable: String,
    private val topicFamily: ByteArray
) {
    companion object {
        fun connect() = HbaseClient(
            ConnectionFactory.createConnection(HBaseConfiguration.create(Config.Hbase.config)),
            Config.Hbase.dataTable,
            Config.Hbase.dataFamily.toByteArray(),
            Config.Hbase.topicTable,
            Config.Hbase.topicFamily.toByteArray()
        )
    }

    fun putVersion(topic: ByteArray, key: ByteArray, body: ByteArray, version: Long) {
        val dataTable = connection.getTable(TableName.valueOf(dataTable))
        val topicTable = connection.getTable(TableName.valueOf(topicTable))

        dataTable.put(Put(key).apply {
            this.addColumn(
                dataFamily,
                topic,
                version,
                body
            )
        })

        topicTable.increment(Increment(key).apply {
            addColumn(
                topicFamily,
                topic,
                1
            )
        })
    }

    fun close() = connection.close()
}