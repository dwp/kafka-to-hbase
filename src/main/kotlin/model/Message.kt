package model


import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import parser.TimestampDeserializer

@JsonIgnoreProperties(ignoreUnknown = true)
data class Message(
    @JsonProperty("_lastModifiedDateTime")
    @JsonDeserialize(using = TimestampDeserializer::class)
    val lastModifiedDateTime: Long
)