import org.apache.kafka.clients.consumer.ConsumerRecord
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.util.*

open class MetadataStoreClient(private val connection: Connection) {

    @Synchronized
    fun recordProcessingAttempt(hbaseId: String, record: ConsumerRecord<ByteArray, ByteArray>, lastUpdated: Long) {
        val rowsInserted = preparedStatement(hbaseId, lastUpdated, record).executeUpdate()
        logger.info("Recorded processing attempt", "rows_inserted", "$rowsInserted")
    }

    private fun preparedStatement(hbaseId: String, lastUpdated: Long, record: ConsumerRecord<ByteArray, ByteArray>) =
        recordProcessingAttemptStatement.apply {
            setString(1, hbaseId)
            setTimestamp(2, Timestamp(lastUpdated))
            setString(3, record.topic())
            setInt(4, record.partition())
            setLong(5, record.offset())
        }


    private val recordProcessingAttemptStatement by lazy {
        connection.prepareStatement("""
            INSERT INTO ucfs (hbase_id, hbase_timestamp, topic_name, kafka_partition, kafka_offset)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent())
    }

    companion object {
        private val isUsingAWS = Config.MetadataStore.isUsingAWS
        private val secretHelper: SecretHelperInterface =  if (isUsingAWS) AWSSecretHelper() else DummySecretHelper()

        fun connect(): MetadataStoreClient {

            val hostname = Config.MetadataStore.properties["rds.endpoint"]
            val port = Config.MetadataStore.properties["rds.port"]
            val jdbcUrl = "jdbc:mysql://$hostname:$port/${Config.MetadataStore.properties.getProperty("database")}"
            val username = Config.MetadataStore.properties.getProperty("user")
            val secretName = Config.MetadataStore.properties.getProperty("rds.password.secret.name")

            logger.info("Connecting to RDS Metadata Store", "jdbc_url", jdbcUrl, "username", username)

            val propertiesWithPassword: Properties = Config.MetadataStore.properties.clone() as Properties

            propertiesWithPassword["password"] = secretHelper.getSecret(secretName)
            return MetadataStoreClient(DriverManager.getConnection(jdbcUrl, propertiesWithPassword))
        }

        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(MetadataStoreClient::class.toString())
    }

    fun close() = connection.close()
}
