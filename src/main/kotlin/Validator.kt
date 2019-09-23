import org.everit.json.schema.Schema
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener

class Validator {
    fun isValid(json: String): Boolean {
        val jsonObject = JSONObject(json)
        val schema = schema()
        schema.validate(jsonObject)
        println(jsonObject)
        return true
    }

    private fun schema(): Schema {
        return schemaLoader().load().build()
    }

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

    private fun schemaObject(): JSONObject {
        val location = Config.Validator.properties["schema.location"]
        println("location: '$location'.")
        return javaClass.getResourceAsStream(Config.Validator.properties["schema.location"] as String).use { inputStream ->
            JSONObject(JSONTokener(inputStream))
        }
    }

    private var _schemaLoader: SchemaLoader? = null
}


fun main() {
    val valid = Validator().isValid("""{
        |   "@type": "hello",
        |   "_id": {},
        |   "collection" : "addresses",
        |   "db": "core",
        |   "dbObject": "abcdefghijklm",
        |   "encryption": {
        |       "keyEncryptionKeyId": "123",
        |       "initialisationVector": "iv",
        |       "encryptedEncryptionKey": "=="
        |   } 
        |}
    """.trimMargin())
    println("$valid")
}