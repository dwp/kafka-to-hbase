
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectRequest
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.text.SimpleDateFormat
import java.util.*

class AwsS3ServiceTest : StringSpec() {
    init {
        "Key and custom metadata set correctly" {
            val amazonS3 = mock<AmazonS3>()
            val awsS3Service = AwsS3Service(amazonS3)
            val payloads = (1..100).map { index ->
                val consumerRecord = mock<ConsumerRecord<ByteArray, ByteArray>> {
                    on { key() } doReturn index.toString().toByteArray()
                }
                HbasePayload(Bytes.toBytes("key-$index"), Bytes.toBytes("body-$index"), payloadTime(index), consumerRecord)
            }
            awsS3Service.putObjects("database:collection", payloads)
            val requestCaptor = argumentCaptor<PutObjectRequest>()
            verify(amazonS3, times(200)).putObject(requestCaptor.capture())
            verifyNoMoreInteractions(amazonS3)
            requestCaptor.allValues.filter { it.key.contains(Regex("latest")) }.forEachIndexed { index, putRequest ->
                putRequest.bucketName shouldBe "ucarchive"
                val hexedId = hexedId(index + 1)
                putRequest.key shouldBe "ucdata_main/latest/database/collection/$hexedId.json"
                validateUserMetadata(putRequest.metadata.userMetadata, index)
            }

            requestCaptor.allValues.filter { !it.key.contains(Regex("latest")) }.forEachIndexed { index, putRequest ->
                putRequest.bucketName shouldBe "ucarchive"
                val hexedId = hexedId(index + 1)
                putRequest.key shouldBe "ucdata_main/${payloadDate(index + 1)}/database/collection/$hexedId/${payloadTime(index + 1)}.json"
                validateUserMetadata(putRequest.metadata.userMetadata, index)
            }
        }
    }

    private fun validateUserMetadata(userMetadata: MutableMap<String, String>, index: Int) {
        userMetadata["kafka_message_id"] shouldBe "${index + 1}"
        userMetadata["hbase_id"] shouldBe "\\x6B\\x65\\x79\\x2D${index + 1}"
        userMetadata["database"] shouldBe "database"
        userMetadata["collection"] shouldBe "collection"
        userMetadata["id"] shouldBe "${index + 1}"
        userMetadata["timestamp"] shouldBe payloadTime(index + 1).toString()
    }

    private fun hexedId(index: Int) = Hex.encodeHexString("key-${index}".toByteArray())
    private fun payloadTime(index: Int) = payloadTimestamp(index).time

    private fun payloadTimestamp(index: Int) =
            SimpleDateFormat("yyyy/MM/dd").apply {
                timeZone = TimeZone.getTimeZone("UTC") }.parse(payloadDate(index))

    private fun payloadDate(index: Int) = "2020/01/%02d".format((index % 20) + 1)
}
