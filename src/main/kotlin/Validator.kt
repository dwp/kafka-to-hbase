import org.everit.json.schema.ValidationException
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener

class Validator {

    fun validate(json: String){
        try {
            val jsonObject = JSONObject(json)
            val schema = schema()
            println(jsonObject)
            schema.validate(jsonObject)
        }
        catch (e: ValidationException) {
            throw InvalidMessageException("Message failed schema validation", e)
        }
    }

    private fun schema() = schemaLoader().load().build()

    @Synchronized
    private fun schemaLoader(): SchemaLoader {
        if (_schemaLoader == null) {
            _schemaLoader = SchemaLoader.builder()
                .schemaJson(schemaObject())
                .draftV7Support()
                .build()
        }
        return _schemaLoader!!
    }

    private fun schemaObject() =
        javaClass.getResourceAsStream(schemaLocation()).use { inputStream ->
            JSONObject(JSONTokener(inputStream))
        }

    private fun schemaLocation() = Config.Validator.properties["schema.location"] as String
    private var _schemaLoader: SchemaLoader? = null
}
//
//
//fun main() {
//    Validator().validate("""{
//        |   "message": {
//        |       "@type": "hello",
//        |       "_id": {
//        |           "declarationId": 1
//        |       },
//        |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
//        |       "collection" : "addresses",
//        |       "db": "core",
//        |       "dbObject": "asd",
//        |       "encryption": {
//        |           "keyEncryptionKeyId": "cloudhsm:7,14",
//        |           "initialisationVector": "iv",
//        |           "encryptedEncryptionKey": "=="
//        |       }
//        |   }
//        |}
//    """.trimMargin())
//}
