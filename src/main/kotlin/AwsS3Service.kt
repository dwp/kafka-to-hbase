
import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.commons.codec.binary.Hex
import java.io.ByteArrayInputStream
import java.text.SimpleDateFormat
import java.util.*
import kotlin.system.measureTimeMillis

class AwsS3Service {

    // K2HB_S3_TIMESTAMPED_PATH = s3://data_bucket/ucdata_main/<yyyy>/<mm>/<dd>/<db>/<collection>/<id-hex>/<timestamp>.json
    // K2HB_S3_LATEST_PATH = s3://data_bucket/ucdata_main/latest/<db>/<collection>/<id-hex>.json
    fun putObjects(payloads: List<HbasePayload>, hbaseTable: String) {
        val (database, collection) = hbaseTable.split(Regex(":"))
        val timeTaken = measureTimeMillis {
            payloads.forEach { payload ->
                val timestamp = SimpleDateFormat("yyyy/MM/dd").format(payload.version)
                val hexedId = Hex.encodeHexString(payload.key)
                val key = "${Config.AwsS3.archiveDirectory}/$timestamp/$database/$collection/$hexedId/${payload.version}.json"
                val putObjectRequest = PutObjectRequest(Config.AwsS3.archiveBucket,
                        key, ByteArrayInputStream(payload.body), ObjectMetadata().apply {
                    contentLength = payload.body.size.toLong()
                    contentType = "application/json"
                    addUserMetadata("kafka_message_id", String(payload.record.key()))
                    addUserMetadata("receipt_time", SimpleDateFormat("yyyy/MM/dd").format(Date()))
                    addUserMetadata("hbase_id", textUtils.printableKey(payload.key))
                    addUserMetadata("database", database.replace('_', '-'))
                    addUserMetadata("collection", collection.replace('_', '-'))
                    addUserMetadata("id", String(payload.key).substring(4))
                    addUserMetadata("timestamp", payload.version.toString())
                })
                client.putObject(putObjectRequest)
            }
        }

        println("Time taken $timeTaken")
    }

    val client by lazy {
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
        const val localstackServiceEndPoint = "http://aws:4566/"
        const val localstackSigningRegion = "eu-west-2"
        const val localstackAccessKey = "accessKey"
        const val localstackSecretKey = "secretKey"
        val textUtils = TextUtils()
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(AwsS3Service::class.toString())
    }
}
