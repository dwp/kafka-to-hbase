
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectRequest
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.InputStreamReader
import java.io.LineNumberReader
import java.text.SimpleDateFormat
import java.util.*
import java.util.zip.GZIPInputStream

class ArchiveAwsS3ServiceTest : StringSpec() {
    init {
        "Batch puts set request parameters correctly" {
            val amazonS3 = mock<AmazonS3>()
            val archiveAwsS3Service = ArchiveAwsS3Service(amazonS3)
            val payloads = hbasePayloads()
            archiveAwsS3Service.putBatch("database:collection", payloads)
            val requestCaptor = argumentCaptor<PutObjectRequest>()
            verify(amazonS3, times(1)).putObject(requestCaptor.capture())
            verifyNoMoreInteractions(amazonS3)
            val request = requestCaptor.firstValue
            request.bucketName shouldBe "ucarchive"
            request.key shouldBe "ucdata_main/${today()}/database/collection/db.database.collection_10_1-100.jsonl.gz"
            val lineReader = LineNumberReader(InputStreamReader(GZIPInputStream(request.inputStream)))

            lineReader.forEachLine {
                it shouldBe messageBody(lineReader.lineNumber).replace('\n', ' ')
            }
        }
    }

    private fun hbasePayloads(): List<HbasePayload>
            = (1..100).map { index ->
                val consumerRecord = mock<ConsumerRecord<ByteArray, ByteArray>> {
                    on { key() } doReturn index.toString().toByteArray()
                    on { topic() } doReturn "db.database.collection"
                    on { offset() } doReturn index.toLong()
                    on { partition() } doReturn 10
                }
                HbasePayload(Bytes.toBytes("key-$index"), messageBody(index).toByteArray(), "testId1", payloadTime(index), "_lastModifiedDateTime", "2020-01-01T00:00:00.000", consumerRecord, 1000L, 2000L)
            }

    private fun messageBody(index: Int) =
        """
        {
            "message": {
                "dbObject": "abcdefghijklmnopqrstuvwxyz" 
            },
            "position": $index 
        }
        """.trimIndent()

    private fun today() = dateFormat().format(Date())
    private fun payloadTime(index: Int) = payloadTimestamp(index).time
    private fun payloadTimestamp(index: Int) = dateFormat().parse(payloadDate(index))
    private fun dateFormat() = SimpleDateFormat("yyyy/MM/dd").apply { timeZone = TimeZone.getTimeZone("UTC") }
    private fun payloadDate(index: Int) = "2020/01/%02d".format((index % 20) + 1)
}
