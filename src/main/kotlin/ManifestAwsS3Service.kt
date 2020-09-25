
import Config.AwsS3.localstackAccessKey
import Config.AwsS3.localstackSecretKey
import Config.AwsS3.localstackServiceEndPoint
import Config.dataworksRegion
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
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.*
import java.util.zip.GZIPOutputStream
import kotlin.system.measureTimeMillis
import org.apache.commons.text.StringEscapeUtils]
import com.beust.klaxon.JsonObject

open class ManifestAwsS3Service(private val amazonS3: AmazonS3) {

    open suspend fun putManifestForBatch(hbaseTable: String, payloads: List<HbasePayload>) {
        if (payloads.isNotEmpty()) {
            val (database, collection) = hbaseTable.split(Regex(":"))
            val key = dateStampedKey(database, collection, "TEMPORARY_UNIQUE_ID")
            logger.info("Putting manifest into s3", "size", "${payloads.size}", "hbase_table", hbaseTable, "key", key)
            val timeTaken = measureTimeMillis { putManifest(key, manifestBody(database, collection, payloads)) }
            logger.info("Put manifest into s3", "time_taken", "$timeTaken", "size", "${payloads.size}", "hbase_table", hbaseTable, "key", key)
        }
    }

    private fun putManifest(key: String, body: ByteArray) =
        amazonS3.putObject(PutObjectRequest(Config.ManifestS3.manifestBucket, key,
                ByteArrayInputStream(body), ObjectMetadata().apply {
            contentLength = body.size.toLong()
        }))

    private fun manifestBody(database: String, collection: String, payloads: List<HbasePayload>) =
        ByteArrayOutputStream().also {
            BufferedOutputStream(GZIPOutputStream(it)).use { bufferedOutputStream ->
                payloads.forEach { payload ->
                    val manifestRecord = manifestRecordForPayload(database, collection, payload)
                    val body = csv(manifestRecord)
                    bufferedOutputStream.write(body.toString().toByteArray(Charset.forName("UTF-8")))
                }
            }
        }.toByteArray()

    private fun manifestRecordForPayload(database: String, collection: String, payload: HbasePayload): ManifestRecord
            = ManifestRecord(payload.id, payload.version, database, collection, 
                MANIFEST_RECORD_SOURCE, MANIFEST_RECORD_COMPONENT, MANIFEST_RECORD_TYPE, payload.id)

    private suspend fun putObject(key: String, payload: HbasePayload, database: String, collection: String)
            = withContext(Dispatchers.IO) { amazonS3.putObject(putObjectRequest(key, payload, database, collection)) }

    // K2HB_MANIFEST_FILE_PATH: s3://manifest/streamed/<yyyy>/<mm>/<dd>/<db>_<collection>_<uniqueid>.json
    private fun dateStampedKey(database: String, collection: String, uniqueId: String)
            = "${Config.ManifestS3.manifestDirectory}/${dateNowPath()}/${database}_${collection}_${uniqueId}.json"

    private fun dateNowPath() = simpleDateFormatter().format(Calendar.getInstance().getTime())

    private fun putObjectRequest(key: String, payload: HbasePayload, database: String, collection: String) =
            PutObjectRequest(Config.ManifestS3.manifestBucket,
                    key, ByteArrayInputStream(payload.body), objectMetadata(payload, database, collection))

    private fun objectMetadata(payload: HbasePayload, database: String, collection: String)
        = ObjectMetadata().apply {
            contentLength = payload.body.size.toLong()
            contentType = "application/json"
            addUserMetadata("kafka_message_id", String(payload.record.key()))

            addUserMetadata("receipt_time", SimpleDateFormat("yyyy/MM/dd HH:mm:ss").apply {
                timeZone = TimeZone.getTimeZone("UTC")
            }.format(Date()))

            addUserMetadata("database", database.replace('_', '-'))
            addUserMetadata("collection", collection.replace('_', '-'))
        }

    private fun csv(manifestRecord: ManifestRecord) =
            "${escape(manifestRecord.id)}|${escape(manifestRecord.timestamp.toString())}|${escape(manifestRecord.db)}|${escape(manifestRecord.collection)}|${escape(manifestRecord.source)}|${escape(manifestRecord.externalOuterSource)}|${escape(manifestRecord.originalId)}|${escape(manifestRecord.externalInnerSource)}\n"

    private fun escape(value: String) = StringEscapeUtils.escapeCsv(value)

    private fun simpleDateFormatter() = SimpleDateFormat("yyyy/MM/dd").apply { timeZone = TimeZone.getTimeZone("UTC") }

    companion object {
        fun connect() = ManifestAwsS3Service(s3)
        val textUtils = TextUtils()
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(ManifestAwsS3Service::class.toString())
        val s3: AmazonS3 by lazy {
            if (Config.AwsS3.useLocalStack) {
                AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(localstackServiceEndPoint, dataworksRegion))
                    .withClientConfiguration(ClientConfiguration().apply {
                        withProtocol(Protocol.HTTP)
                        maxConnections = Config.AwsS3.maxConnections
                    })
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
        val MANIFEST_RECORD_SOURCE = "STREAMED"
        val MANIFEST_RECORD_COMPONENT = "K2HB"
        val MANIFEST_RECORD_TYPE = "KAFKA_RECORD"
    }
}
