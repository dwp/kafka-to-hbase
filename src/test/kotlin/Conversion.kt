import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.properties.assertAll
import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.specs.StringSpec
import com.beust.klaxon.JsonObject

class Conversion : StringSpec({
    configureLogging()

    "valid input converts to json" {
        val json_string = "{\"testOne\":\"test1\", \"testTwo\":2}"
        
        val json: JsonObject = convertToJson(json_string.toByteArray())

        json should beInstanceOf<JsonObject>()
        json.string("testOne") shouldBe "test1"
        json.int("testTwo") shouldBe 2
    }

    "valid nested input converts to json" {
        val json_string = "{\"testOne\":{\"testTwo\":2}}"
        
        val json: JsonObject = convertToJson(json_string.toByteArray())
        val json_two: JsonObject = json.obj("testOne") as JsonObject

        json should beInstanceOf<JsonObject>()
        json_two.int("testTwo") shouldBe 2
    }

    "invalid nested input throws exception" {
        val json_string = "{\"testOne\":"

        val exception = shouldThrow<IllegalArgumentException> {
            convertToJson(json_string.toByteArray())
        }
        
        exception.message shouldBe "Cannot parse invalid JSON"
    }

    "can generate consistent base64 encoded string" {
        val json_string_with_fake_hash = "82&%\$dsdsd{\"testOne\":\"test1\", \"testTwo\":2}"
        
        val encodedStringOne = encodeToBase64(json_string_with_fake_hash)
        val encodedStringTwo = encodeToBase64(json_string_with_fake_hash)

        encodedStringOne shouldBe encodedStringTwo
    }

    "can encode and decode string with base64" {
        val json_string_with_fake_hash = "82&%\$dsdsd{\"testOne\":\"test1\", \"testTwo\":2}"
        
        val encodedString = encodeToBase64(json_string_with_fake_hash)
        val decodedString = decodeFromBase64(encodedString)

        decodedString shouldBe json_string_with_fake_hash
    }

    "sorts json by key name" {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testb\":true}"
        val jsonObjectUnsorted: JsonObject = convertToJson(jsonStringUnsorted.toByteArray())
        val jsonStringSorted = "testA=test1,testb=true,testC=2"

        val sortedJson = sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    "checksums are different with different inputs" {
        val jsonStringOne = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val jsonStringTwo = "{\"testOne\":\"test2\", \"testTwo\":2}"
        val checksum = generateFourByteChecksum(jsonStringOne)
        val checksumTwo = generateFourByteChecksum(jsonStringTwo)

        checksum shouldNotBe checksumTwo
    }

    "can generate consistent checksums from json" {
        val json_string = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = convertToJson(json_string.toByteArray())
        val checksumOne = generateFourByteChecksum(json.toString())
        val checksumTwo = generateFourByteChecksum(json.toString())

        checksumOne shouldBe checksumTwo
    }

    "generated checksums are four bytes" {
        assertAll({ input: String ->
            val checksum = generateFourByteChecksum(input)
            checksum.size shouldBe 4
        })
    }
})