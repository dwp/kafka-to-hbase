package parser

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule


class JacksonObjectMapper {

    companion object {

        private var INSTANCE: ObjectMapper? = null
        val instance: ObjectMapper
            get() {
                if (INSTANCE == null) {
                    INSTANCE = ObjectMapper().registerModule(KotlinModule())
                }
                return INSTANCE!!
            }
    }
}
