import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import com.beust.klaxon.JsonObject

class MessageParserTest : StringSpec({
    configureLogging()

    val convertor = Convertor()

    "generated keys are consistent for identical inputs" {
        val parser = MessageParser()
        val json: JsonObject = convertor.convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())
        
        val keyOne: ByteArray = parser.generateKey(json)
        val keyTwo: ByteArray = parser.generateKey(json)

        keyOne shouldBe keyTwo
    }

    "generated keys are different for different inputs" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())
        val jsonTwo: JsonObject = convertor.convertToJson("{\"testOne\":\"test1\", \"testTwo\":3}".toByteArray())
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne shouldNotBe keyTwo
    }

    "generated keys are consistent for identical inputs regardless of order" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())
        val jsonTwo: JsonObject = convertor.convertToJson("{\"testTwo\":2, \"testOne\":\"test1\"}".toByteArray())
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne shouldBe keyTwo
    }

    "generated keys are consistent for identical inputs regardless of whitespace" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())
        val jsonTwo: JsonObject = convertor.convertToJson("{    \"testOne\":              \"test1\",            \"testTwo\":  2}".toByteArray())
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne shouldBe keyTwo
    }

    "generated keys are consistent for identical inputs regardless of order and whitespace" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())
        val jsonTwo: JsonObject = convertor.convertToJson("{    \"testTwo\":              2,            \"testOne\":  \"test1\"}".toByteArray())
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne shouldBe keyTwo
    }

    "generated keys will vary given values with different whitespace" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())
        val jsonTwo: JsonObject = convertor.convertToJson("{\"testOne\":\"test 1\", \"testTwo\":2}".toByteArray())
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne shouldNotBe keyTwo
    }

    "id is returned from valid json" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"message\":{\"_id\":{\"test_key\":\"test_value\"}}}".toByteArray())
        val idString = "{\"test_key\":\"test_value\"}"

        val idJson: JsonObject? = parser.getId(jsonOne)

        idJson?.toJsonString() shouldBe idString
    }

    "null is returned from json where message does not exist" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"test_object\":{\"_id\":{\"test_key\":\"test_value\"}}}".toByteArray())
        val idJson: JsonObject? = parser.getId(jsonOne)

        idJson shouldBe null
    }

    "null is returned from json where message is not an object" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"message\":\"test_value\"}".toByteArray())
        val idJson: JsonObject? = parser.getId(jsonOne)

        idJson shouldBe null
    }

    "null is returned from json where _id is missing" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"message\":{\"test_object\":{\"test_key\":\"test_value\"}}}".toByteArray())
        val idJson: JsonObject? = parser.getId(jsonOne)

        idJson shouldBe null
    }

    "null is returned from json where _id is not an object" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"message\":{\"_id\":\"test_value\"}}".toByteArray())
        val idJson: JsonObject? = parser.getId(jsonOne)

        idJson shouldBe null
    }

    "generated key is consistent for identical record body independant of key order and whitespace" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"message\":{\"_id\":{\"test_key_a\":\"test_value_a\",\"test_key_b\"    :\"test_value_b\"}}}".toByteArray())
        val jsonTwo: JsonObject = convertor.convertToJson("{\"message\":{\"_id\":{\"test_key_b\":     \"test_value_b\",\"test_key_a\":\"test_value_a\"}}}".toByteArray())
        
        val keyOne: ByteArray = parser.generateKeyFromRecordBody(jsonOne)
        val keyTwo: ByteArray = parser.generateKeyFromRecordBody(jsonTwo)

        keyOne shouldNotBe ByteArray(0)
        keyOne shouldBe keyTwo
    }

    "empty is returned from record body key generation where message does not exist" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"test_object\":{\"_id\":{\"test_key\":\"test_value\"}}}".toByteArray())
        val key: ByteArray = parser.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    "empty is returned from record body key generation where message is not an object" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"message\":\"test_value\"}".toByteArray())
        val key: ByteArray = parser.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    "empty is returned from record body key generation where _id is missing" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"message\":{\"test_object\":{\"test_key\":\"test_value\"}}}".toByteArray())
        val key: ByteArray = parser.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    "empty is returned from record body key generation where _id is not an object" {
        val parser = MessageParser()
        val jsonOne: JsonObject = convertor.convertToJson("{\"message\":{\"_id\":\"test_value\"}}".toByteArray())
        val key: ByteArray = parser.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }
})