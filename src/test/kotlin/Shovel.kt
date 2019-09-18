import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec

class Shovel : StringSpec({
    configureLogging()

    "generated key is consistent for identical inputs" {
        val json_string = "{\"testOne\":\"test1\", \"testTwo\":2}"
        
        val keyOne: ByteArray = generateKey(json_string.toByteArray())
        val keyTwo: ByteArray = generateKey(json_string.toByteArray())

        keyOne shouldBe keyTwo
    }

    "generated keys are different for different inputs" {
        val json_string_one = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json_string_two = "{\"testOne\":\"test1\", \"testTwo\":3}"
        
        val keyOne: ByteArray = generateKey(json_string_one.toByteArray())
        val keyTwo: ByteArray = generateKey(json_string_two.toByteArray())

        keyOne shouldNotBe keyTwo
    }

    "generated key is consistent for identical inputs regardless of order" {
        val json_string_one = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json_string_two = "{\"testTwo\":2, \"testOne\":\"test1\"}"
        
        val keyOne: ByteArray = generateKey(json_string_one.toByteArray())
        val keyTwo: ByteArray = generateKey(json_string_two.toByteArray())

        keyOne shouldBe keyTwo
    }

    "generated key is consistent for identical inputs regardless of whitespace" {
        val json_string_one = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json_string_two = "{    \"testOne\":              \"test1\",            \"testTwo\":  2}"
        
        val keyOne: ByteArray = generateKey(json_string_one.toByteArray())
        val keyTwo: ByteArray = generateKey(json_string_two.toByteArray())

        keyOne shouldBe keyTwo
    }

    "generated key is consistent for identical inputs regardless of order and whitespace" {
        val json_string_one = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json_string_two = "{    \"testTwo\":              2,            \"testOne\":  \"test1\"}"
        
        val keyOne: ByteArray = generateKey(json_string_one.toByteArray())
        val keyTwo: ByteArray = generateKey(json_string_two.toByteArray())

        keyOne shouldBe keyTwo
    }

    "empty key returned for invalid json" {
        val json_string = "{\"testOne\":}"
        
        val keyOne: ByteArray = generateKey(json_string.toByteArray())

        keyOne.isEmpty() shouldBe true
    }
})