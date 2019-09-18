package parser

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.module.kotlin.readValue
import configureLogging
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import model.Record

class JacksonObjectMapperTest : StringSpec({
    configureLogging()

    "Valid _lastModifiedDateTime gets parsed correctly" {
        val json = "{\n" +
            "        \"traceId\": \"00001111-abcd-4567-1234-1234567890ab\",\n" +
            "        \"unitOfWorkId\": \"00002222-abcd-4567-1234-1234567890ab\",\n" +
            "        \"@type\": \"V4\",\n" +
            "        \"version\": \"core-X.release_XXX.XX\",\n" +
            "        \"timestamp\": \"2018-12-14T15:01:02.000+0000\",\n" +
            "        \"message\": {\n" +
            "            \"@type\": \"MONGO_UPDATE\",\n" +
            "            \"collection\": \"exampleCollectionName\",\n" +
            "            \"db\": \"exampleDbName\",\n" +
            "            \"_id\": {\n" +
            "                \"exampleId\": \"aaaa1111-abcd-4567-1234-1234567890ab\"\n" +
            "            },\n" +
            "            \"_lastModifiedDateTime\": \"2018-12-15T15:01:02.000+0000\",\n" +
            "            \"encryption\": {\n" +
            "                \"encryptionKeyId\": \"55556666-abcd-89ab-1234-1234567890ab\",\n" +
            "                \"encryptedEncryptionKey\": \"bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab\",\n" +
            "                \"initialisationVector\": \"kjGyvY67jhJHVdo2\",\n" +
            "                \"keyEncryptionKeyId\": \"example-key_2019-12-14_01\"\n" +
            "            },\n" +
            "            \"dbObject\": \"bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A\"\n" +
            "        }\n" +
            "    }"

        val record: Record = JacksonObjectMapper.instance.readValue(json)
        record.message.lastModifiedDateTime shouldBe 1544886062000
    }

    "Invalid _lastModifiedDateTime throws json mapping exception" {
        val json = "{\n" +
            "        \"traceId\": \"00001111-abcd-4567-1234-1234567890ab\",\n" +
            "        \"unitOfWorkId\": \"00002222-abcd-4567-1234-1234567890ab\",\n" +
            "        \"@type\": \"V4\",\n" +
            "        \"version\": \"core-X.release_XXX.XX\",\n" +
            "        \"timestamp\": \"2018-12-14T15:01:02.000+0000\",\n" +
            "        \"message\": {\n" +
            "            \"@type\": \"MONGO_UPDATE\",\n" +
            "            \"collection\": \"exampleCollectionName\",\n" +
            "            \"db\": \"exampleDbName\",\n" +
            "            \"_id\": {\n" +
            "                \"exampleId\": \"aaaa1111-abcd-4567-1234-1234567890ab\"\n" +
            "            },\n" +
            "            \"_lastModifiedDateTime\": \"2018-12-15T15:01:02\",\n" +
            "            \"encryption\": {\n" +
            "                \"encryptionKeyId\": \"55556666-abcd-89ab-1234-1234567890ab\",\n" +
            "                \"encryptedEncryptionKey\": \"bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab\",\n" +
            "                \"initialisationVector\": \"kjGyvY67jhJHVdo2\",\n" +
            "                \"keyEncryptionKeyId\": \"example-key_2019-12-14_01\"\n" +
            "            },\n" +
            "            \"dbObject\": \"bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A\"\n" +
            "        }\n" +
            "    }"
        shouldThrow<JsonMappingException> {
            val record: Record = JacksonObjectMapper.instance.readValue(json)
            record.message.lastModifiedDateTime shouldBe 1544886062000
        }
    }
}
)