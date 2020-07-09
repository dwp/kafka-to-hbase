import com.amazonaws.services.secretsmanager.*
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.fasterxml.jackson.databind.ObjectMapper

class DummySecretHelper {

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(DummySecretHelper::class.toString())
    }

    val dummySecretsMap = mutableMapOf<String, String>()

    fun getSecret(secretName: String): String? {

        logger.info("Getting value from dummy secret manager", "secret_name", secretName)

        try {
            return dummySecretsMap[secretName]
        } catch (e: Exception) {
            logger.error("Failed to get dummy secret manager result", e, "secret_name", secretName)
            throw e
        }
    }
}
