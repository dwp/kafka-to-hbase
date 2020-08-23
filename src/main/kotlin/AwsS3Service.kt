

import Config.AwsS3.localstackAccessKey
import Config.AwsS3.localstackSecretKey
import Config.AwsS3.localstackServiceEndPoint
import Config.AwsS3.localstackSigningRegion
import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.commons.codec.binary.Hex
import java.io.ByteArrayInputStream
import java.text.SimpleDateFormat
import java.util.*
import kotlin.system.measureTimeMillis

class AwsS3Service {

    // K2HB_S3_TIMESTAMPED_PATH = s3://data_bucket/ucdata_main/<yyyy>/<mm>/<dd>/<db>/<collection>/<id-hex>/<timestamp>.json
    // K2HB_S3_LATEST_PATH = s3://data_bucket/ucdata_main/latest/<db>/<collection>/<id-hex>.json
    suspend fun putObjects(hbaseTable: String, payloads: List<HbasePayload>) {
        val timeTaken = measureTimeMillis {
            logger.info("Putting batch into s3", "size", "${payloads.size}", "hbase_table", hbaseTable)
            val (database, collection) = hbaseTable.split(Regex(":"))
            coroutineScope {
                payloads.forEach { payload ->
                    if (Config.AwsS3.parallelPuts) {
                        launch { putPayload(database, collection, payload) }
                    } else {
                        putPayload(database, collection, payload)
                    }
                }
            }
        }
        logger.info("Put batch into s3", "time_taken", "$timeTaken", "size", "${payloads.size}", "hbase_table", hbaseTable)
    }

    private suspend fun putPayload(database: String, collection: String, payload: HbasePayload)
            = withContext(Dispatchers.IO) {
                val timestamp = SimpleDateFormat("yyyy/MM/dd").format(payload.version)
                val hexedId = Hex.encodeHexString(payload.key)
                val key = "${Config.AwsS3.archiveDirectory}/$timestamp/$database/$collection/$hexedId/${payload.version}.json"
    //            logger.info("Putting object into s3", "key", key)
                client.putObject(putObjectRequest(key, payload, database, collection))
    //            logger.info("Put object into s3", "key", key)
            }

    private fun putObjectRequest(key: String, payload: HbasePayload, database: String, collection: String) =
            PutObjectRequest(Config.AwsS3.archiveBucket,
                    key, ByteArrayInputStream(payload.body), objectMetadata(payload, database, collection))

    private fun objectMetadata(payload: HbasePayload, database: String, collection: String)
        = ObjectMetadata().apply {
            contentLength = payload.body.size.toLong()
            contentType = "application/json"
            addUserMetadata("kafka_message_id", String(payload.record.key()))
            addUserMetadata("receipt_time", SimpleDateFormat("yyyy/MM/dd").format(Date()))
            addUserMetadata("hbase_id", textUtils.printableKey(payload.key))
            addUserMetadata("database", database.replace('_', '-'))
            addUserMetadata("collection", collection.replace('_', '-'))
            addUserMetadata("id", String(payload.key).substring(4))
            addUserMetadata("timestamp", payload.version.toString())
        }

    val client: AmazonS3 by lazy {
        if (Config.AwsS3.useLocalStack) {
            AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(localstackServiceEndPoint, localstackSigningRegion))
                    .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
                    .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(localstackAccessKey, localstackSecretKey)))
                    .withPathStyleAccessEnabled(true)
                    .disableChunkedEncoding()
                    .build()
        }
        else {
            AmazonS3ClientBuilder.standard()
                    .withCredentials(DefaultAWSCredentialsProviderChain())
                    .withRegion(Config.AwsS3.region)
                    .withClientConfiguration(ClientConfiguration().apply {
                        maxConnections = Config.AwsS3.maxConnections
                    })
                    .build()
        }
    }

    private companion object {
        val textUtils = TextUtils()
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(AwsS3Service::class.toString())
    }
}
