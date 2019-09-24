import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import com.beust.klaxon.JsonObject
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doNothing
import com.nhaarman.mockitokotlin2.spy
import com.nhaarman.mockitokotlin2.whenever
import org.apache.kafka.clients.consumer.ConsumerRecord

class MessageParserTest : StringSpec({
    configureLogging()

    val convertor = spy(Converter())
    doNothing().whenever(convertor).sendMessageToDlq(any())

    "generated keys are consistent for identical inputs" {
        val parser = MessageParser()
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val consumerRecord = ConsumerRecord("",0,1,"key".toByteArray(),jsonString.toByteArray())
        val json: JsonObject = convertor.convertToJson(consumerRecord)
        
        val keyOne: ByteArray = parser.generateKey(json)
        val keyTwo: ByteArray = parser.generateKey(json)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    "generated keys are different for different inputs" {
        val parser = MessageParser()
        val jsonString1 = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonString2 = "{\"testOne\":\"test1\", \"testTwo\":3}"
        val consumerRecord2 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString2.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val jsonTwo: JsonObject = convertor.convertToJson(consumerRecord2)

        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "generated keys are consistent for identical inputs regardless of order" {
        val parser = MessageParser()

        val jsonString1 = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonString2 = "{\"testTwo\":2, \"testOne\":\"test1\"}"
        val consumerRecord2 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString2.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val jsonTwo: JsonObject = convertor.convertToJson(consumerRecord2)
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    "generated keys are consistent for identical inputs regardless of whitespace" {
        val parser = MessageParser()
        val jsonString1 = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonString2 = "{    \"testOne\":              \"test1\",            \"testTwo\":  2}"
        val consumerRecord2 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString2.toByteArray())

        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val jsonTwo: JsonObject = convertor.convertToJson(consumerRecord2)
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    "generated keys are consistent for identical inputs regardless of order and whitespace" {
        val parser = MessageParser()

        val jsonString1 = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonString2 = "{    \"testTwo\":              2,            \"testOne\":  \"test1\"}"
        val consumerRecord2 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString2.toByteArray())

        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val jsonTwo: JsonObject = convertor.convertToJson(consumerRecord2)
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    "generated keys will vary given values with different whitespace" {
        val parser = MessageParser()

        val jsonString1 = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonString2 = "{\"testOne\":\"test 1\", \"testTwo\":2}"
        val consumerRecord2 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString2.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val jsonTwo: JsonObject = convertor.convertToJson(consumerRecord2)
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "generated keys will vary given values that are string and int in each input" {
        val parser = MessageParser()
        val jsonString1 = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonString2 = "{\"testOne\":\"test1\", \"testTwo\":\"2\"}"
        val consumerRecord2 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString2.toByteArray())

        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val jsonTwo: JsonObject = convertor.convertToJson(consumerRecord2)
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "generated keys will vary given values that are string and float in each input" {
        val parser = MessageParser()
        val jsonString1 = "{\"testOne\":\"test1\", \"testTwo\":2.0}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonString2 = "{\"testOne\":\"test1\", \"testTwo\":\"2.0\"}"
        val consumerRecord2 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString2.toByteArray())

        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val jsonTwo: JsonObject = convertor.convertToJson(consumerRecord2)
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "generated keys will vary given values that are string and boolean in each input" {
        val parser = MessageParser()

        val jsonString1 = "{\"testOne\":\"test1\", \"testTwo\":false}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonString2 = "{\"testOne\":\"test1\", \"testTwo\":\"false\"}"
        val consumerRecord2 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString2.toByteArray())

        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val jsonTwo: JsonObject = convertor.convertToJson(consumerRecord2)
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "generated keys will vary given values that are string and null in each input" {
        val parser = MessageParser()
        val jsonString1 = "{\"testOne\":\"test1\", \"testTwo\":null}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonString2 = "{\"testOne\":\"test1\", \"testTwo\":\"null\"}"
        val consumerRecord2 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString2.toByteArray())

        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val jsonTwo: JsonObject = convertor.convertToJson(consumerRecord2)
        
        val keyOne: ByteArray = parser.generateKey(jsonOne)
        val keyTwo: ByteArray = parser.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    "id is returned from valid json" {
        val parser = MessageParser()
        val jsonString1 = "{\"message\":{\"_id\":{\"test_key\":\"test_value\"}}}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val idString = "{\"test_key\":\"test_value\"}"

        val idJson: JsonObject? = parser.getId(jsonOne)

        idJson?.toJsonString() shouldBe idString
    }

    "null is returned from json where message does not exist" {
        val parser = MessageParser()
        val jsonString1 = "{\"test_object\":{\"_id\":{\"test_key\":\"test_value\"}}}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val idJson: JsonObject? = parser.getId(jsonOne)

        idJson shouldBe null
    }

    "null is returned from json where message is not an object" {
        val parser = MessageParser()
        val jsonString1 = "{\"message\":\"test_value\"}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val idJson: JsonObject? = parser.getId(jsonOne)

        idJson shouldBe null
    }

    "null is returned from json where _id is missing" {
        val parser = MessageParser()
        val jsonString1 = "{\"message\":{\"test_object\":{\"test_key\":\"test_value\"}}}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val idJson: JsonObject? = parser.getId(jsonOne)

        idJson shouldBe null
    }

    "null is returned from json where _id is not an object" {
        val parser = MessageParser()
        val jsonString1 = "{\"message\":{\"_id\":\"test_value\"}}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val idJson: JsonObject? = parser.getId(jsonOne)

        idJson shouldBe null
    }

    "generated key is consistent for identical record body independant of key order and whitespace" {
        val parser = MessageParser()
        val jsonString1 = "{\"message\":{\"_id\":{\"test_key_a\":\"test_value_a\",\"test_key_b\"    :\"test_value_b\"}}}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonString2 = "{\"message\":{\"_id\":{\"test_key_b\":     \"test_value_b\",\"test_key_a\":\"test_value_a\"}}}"
        val consumerRecord2 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString2.toByteArray())

        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val jsonTwo: JsonObject = convertor.convertToJson(consumerRecord2)
        
        val keyOne: ByteArray = parser.generateKeyFromRecordBody(jsonOne)
        val keyTwo: ByteArray = parser.generateKeyFromRecordBody(jsonTwo)

        keyOne shouldNotBe ByteArray(0)
        keyOne shouldBe keyTwo
    }

    "empty is returned from record body key generation where message does not exist" {
        val parser = MessageParser()
        val jsonString1 = "{\"test_object\":{\"_id\":{\"test_key\":\"test_value\"}}}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val key: ByteArray = parser.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    "empty is returned from record body key generation where message is not an object" {
        val parser = MessageParser()
        val jsonString1 = "{\"message\":\"test_value\"}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val key: ByteArray = parser.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    "empty is returned from record body key generation where _id is missing" {
        val parser = MessageParser()
        val jsonString1 = "{\"message\":{\"test_object\":{\"test_key\":\"test_value\"}}}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())
        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val key: ByteArray = parser.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    "empty is returned from record body key generation where _id is not an object" {
        val parser = MessageParser()
        val jsonString1 = "{\"message\":{\"_id\":\"test_value\"}}"
        val consumerRecord1 = ConsumerRecord("",0,1,"key".toByteArray(),jsonString1.toByteArray())

        val jsonOne: JsonObject = convertor.convertToJson(consumerRecord1)
        val key: ByteArray = parser.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }
})
