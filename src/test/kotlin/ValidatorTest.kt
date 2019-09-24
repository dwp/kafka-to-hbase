import io.kotlintest.matchers.string.shouldStartWith
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec

class ValidatorTest: StringSpec({

    configureLogging()

    "Valid message passes validation." {
        Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "collection" : "addresses",
            |       "db": "core",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
        """.trimMargin())
    }

    "Missing message causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "msg": {
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation"
    }

    "Missing @type field causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "collection" : "addresses",
            |       "db": "core",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation"
    }

})