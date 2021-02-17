
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.*
import java.util.zip.GZIPOutputStream
import kotlin.system.measureTimeMillis

open class ArchiveAwsS3Service(private val amazonS3: AmazonS3) {

    open suspend fun putBatch(hbaseTable: String, payloads: List<HbasePayload>) {
        if (payloads.isNotEmpty()) {
            val (database, collection) = hbaseTable.split(Regex(":"))
            val key = batchKey(database, collection, payloads)
            logger.info("Putting batch into s3", "size" to "${payloads.size}", "hbase_table" to hbaseTable, "key" to key)
            val timeTaken = measureTimeMillis { putBatchObject(key, batchBody(payloads)) }
            logger.info("Put batch into s3", "time_taken" to "$timeTaken", "size" to  "${payloads.size}",
                "hbase_table" to hbaseTable, "key" to key)
        }
    }

    private fun putBatchObject(key: String, body: ByteArray) =
        amazonS3.putObject(PutObjectRequest(Config.ArchiveS3.archiveBucket, key,
                ByteArrayInputStream(body), ObjectMetadata().apply {
            contentLength = body.size.toLong()
        }))


    private fun batchBody(payloads: List<HbasePayload>) =
        ByteArrayOutputStream().also {
            BufferedOutputStream(GZIPOutputStream(it)).use { bufferedOutputStream ->
                payloads.forEach { payload ->
                    val body = StringBuilder(String(payload.body, Charset.forName("UTF-8"))
                            .replace("\n", " ")).append('\n').toString()
                    bufferedOutputStream.write(body.toByteArray(Charset.forName("UTF-8")))
                }
            }
        }.toByteArray()


    private fun batchKey(database: String, collection: String, payloads: List<HbasePayload>): String {
        val firstRecord = payloads.first().record
        val last = payloads.last().record
        val partition = firstRecord.partition()
        val firstOffset = firstRecord.offset()
        val lastOffset =  last.offset()
        val topic = firstRecord.topic()
        val filename = "${topic}_${partition}_$firstOffset-$lastOffset"
        return "${Config.ArchiveS3.archiveDirectory}/${simpleDateFormatter().format(Date())}/$database/$collection/$filename.jsonl.gz"
    }

    private fun simpleDateFormatter() = SimpleDateFormat("yyyy/MM/dd").apply { timeZone = TimeZone.getTimeZone("UTC") }

    companion object {
        fun connect() = ArchiveAwsS3Service(s3)
        val textUtils = TextUtils()
        val logger = DataworksLogger.getLogger(ArchiveAwsS3Service::class)
        val s3 = Config.AwsS3.s3
    }
}
