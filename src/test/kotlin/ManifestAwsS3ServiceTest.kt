
// import com.amazonaws.services.s3.AmazonS3
// import com.amazonaws.services.s3.model.PutObjectRequest
// import com.nhaarman.mockitokotlin2.*
// import io.kotest.core.spec.style.StringSpec
// import io.kotest.matchers.shouldBe
// import org.apache.commons.codec.binary.Hex
// import org.apache.hadoop.hbase.util.Bytes
// import org.apache.kafka.clients.consumer.ConsumerRecord
// import java.io.InputStreamReader
// import java.io.LineNumberReader
// import java.text.SimpleDateFormat
// import java.util.*
// import java.util.zip.GZIPInputStream

// class ManifestAwsS3ServiceTest : StringSpec() {
//     init {
//         "Key and custom metadata set correctly on individual puts" {
//             val amazonS3 = mock<AmazonS3>()
//             val manifestAwsS3Service = ManifestAwsS3Service(amazonS3)
//             val payloads = hbasePayloads()
//             manifestAwsS3Service.putObjects("database:collection", payloads)
//             val requestCaptor = argumentCaptor<PutObjectRequest>()
//             verify(amazonS3, times(200)).putObject(requestCaptor.capture())
//             verifyNoMoreInteractions(amazonS3)
//             requestCaptor.allValues.filter { it.key.contains(Regex("latest")) }.forEachIndexed { index, putRequest ->
//                 putRequest.bucketName shouldBe "manifests"
//                 val hexedId = hexedId(index + 1)
//                 putRequest.key shouldBe "ucdata_main/latest/database/collection/$hexedId.json"
//                 validateUserMetadata(putRequest.metadata.userMetadata, index)
//             }

//             requestCaptor.allValues.filter { !it.key.contains(Regex("latest")) }.forEachIndexed { index, putRequest ->
//                 putRequest.bucketName shouldBe "manifests"
//                 val hexedId = hexedId(index + 1)
//                 putRequest.key shouldBe "ucdata_main/${payloadDate(index + 1)}/database/collection/$hexedId/${payloadTime(index + 1)}.json"
//                 validateUserMetadata(putRequest.metadata.userMetadata, index)
//             }
//         }

//         "Batch puts set request parameters correctly" {
//             val amazonS3 = mock<AmazonS3>()
//             val manifestAwsS3Service = manifestAwsS3Service(amazonS3)
//             val payloads = hbasePayloads()
//             manifestAwsS3Service.putObjectsAsBatch("database:collection", payloads)
//             val requestCaptor = argumentCaptor<PutObjectRequest>()
//             verify(amazonS3, times(1)).putObject(requestCaptor.capture())
//             verifyNoMoreInteractions(amazonS3)
//             val request = requestCaptor.firstValue
//             request.bucketName shouldBe "manifests"
//             request.key shouldBe "ucdata_main/${today()}/database/collection/db.database.collection_10_1-100.jsonl.gz"
//             val lineReader = LineNumberReader(InputStreamReader(GZIPInputStream(request.inputStream)))

//             lineReader.forEachLine {
//                 it shouldBe messageBody(lineReader.lineNumber).replace('\n', ' ')
//             }
//         }
//     }

//     private fun hbasePayloads(): List<HbasePayload>
//             = (1..100).map { index ->
//                 val consumerRecord = mock<ConsumerRecord<ByteArray, ByteArray>> {
//                     on { key() } doReturn index.toString().toByteArray()
//                     on { topic() } doReturn "db.database.collection"
//                     on { offset() } doReturn index.toLong()
//                     on { partition() } doReturn 10
//                 }
//                 HbasePayload(Bytes.toBytes("key-$index"), messageBody(index).toByteArray(), payloadTime(index), consumerRecord)
//             }

//     private fun messageBody(index: Int) =
//         """
//         {
//             "message": {
//                 "dbObject": "abcdefghijklmnopqrstuvwxyz" 
//             },
//             "position": $index 
//         }
//         """.trimIndent()

//     private fun validateUserMetadata(userMetadata: MutableMap<String, String>, index: Int) {
//         userMetadata["database"] shouldBe "database"
//         userMetadata["collection"] shouldBe "collection"
//     }

//     private fun today() = dateFormat().format(Date())
//     private fun hexedId(index: Int) = Hex.encodeHexString("key-${index}".toByteArray())
//     private fun payloadTime(index: Int) = payloadTimestamp(index).time
//     private fun payloadTimestamp(index: Int) = dateFormat().parse(payloadDate(index))
//     private fun dateFormat() = SimpleDateFormat("yyyy/MM/dd").apply { timeZone = TimeZone.getTimeZone("UTC") }
//     private fun payloadDate(index: Int) = "2020/01/%02d".format((index % 20) + 1)
// }
