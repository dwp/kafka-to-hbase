import java.util.*

class TestUtils {

    companion object {
        fun defaultMessageValidator(){
            Config.Validator.properties = Properties().apply {
                put("schema.location", "message.schema.json")
            }
        }

        fun equalityMessageValidator(){
            Config.Validator.properties = Properties().apply {
                put("schema.location", "equality_message.schema.json")
            }
        }
    }

}
