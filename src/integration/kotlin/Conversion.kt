import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import lib.*
import org.apache.kafka.clients.producer.KafkaProducer
import kotlin.js.json

class Conversion : StringSpec({
    configureLogging()

    "valid input converts to json" {
        json_string = "{\"testOne\":\"test1\", \"testTwo\":2}"
        
        Json json = convertToJson(json_string.toByteArray())

        json["testOne"] shouldBe "test1"
        json["testTwo"] shouldBe 2
    }
})