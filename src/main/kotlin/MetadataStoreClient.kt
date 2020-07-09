import java.sql.Connection
import java.sql.DriverManager
import java.util.*

open class MetadataStoreClient(var connection: Connection) {

    companion object {

        private val useAwsSecrets = Config.MetadataStore.useAwsSecrets
        private val secretHelper: SecretHelperInterface =  if (useAwsSecrets) AWSSecretHelper() else DummySecretHelper()

        fun connect(): MetadataStoreClient {

            val hostname = Config.MetadataStore.properties["rds.endpoint"]
            val port = Config.MetadataStore.properties["rds.port"]
            val jdbcUrl = "jdbc:mysql://$hostname:$port/"
            val secretName = Config.MetadataStore.properties.getProperty("rds.password.secret.name")

            logger.info("Connecting to RDS Metadata Store", "jdbc_url", jdbcUrl)

            val propertiesWithPassword: Properties = Config.MetadataStore.properties.clone() as Properties

            propertiesWithPassword["password"] = secretHelper.getSecret(secretName)

            return MetadataStoreClient(DriverManager.getConnection(jdbcUrl, propertiesWithPassword))
        }

        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(MetadataStoreClient::class.toString())
    }

    fun close() = connection.close()
}
