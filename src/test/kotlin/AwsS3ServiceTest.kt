
import com.amazonaws.services.s3.AmazonS3
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord

class AwsS3ServiceTest : StringSpec({
        "Works" {
            val amazonS3 = mock<AmazonS3>()
            val awsS3Service = AwsS3Service(amazonS3)
            val payloads = (1..100).map { index ->
                val consumerRecord = mock<ConsumerRecord<ByteArray, ByteArray>> {
                    on { key() } doReturn index.toString().toByteArray()
                }
                HbasePayload(Bytes.toBytes("key-$index"), Bytes.toBytes("body-$index"), index.toLong(), consumerRecord)
            }
            awsS3Service.putObjects("database:collection", payloads)
            verify(amazonS3, times(200)).putObject(any())
        }
})
