import com.beust.klaxon.JsonObject
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer

class ListProcessor(validator: Validator, private val converter: Converter): BaseProcessor(validator, converter) {

    fun processList(hbase: HbaseClient, consumer: KafkaConsumer<ByteArray, ByteArray>, parser: MessageParser, records: ConsumerRecords<ByteArray, ByteArray>) {
        records.partitions().forEach { partition ->
            processList(hbase, parser, partition.topic(), records.records(partition))
        }
    }

    private fun processList(hbase: HbaseClient, parser: MessageParser, topic: String, records: List<ConsumerRecord<ByteArray, ByteArray>>) {
        textUtils.qualifiedTableName(topic)?.let { table ->
            hbase.putList(table, payloads(records, parser))
        }
    }

    private fun payloads(records: List<ConsumerRecord<ByteArray, ByteArray>>, parser: MessageParser): List<HbasePayload>
            = records.mapNotNull { record ->
        convertAndValidateJsonRecord(record)?.let { json ->
            val formattedKey = parser.generateKeyFromRecordBody(json)
            if (formattedKey.isEmpty()) null
            else {
                hbasePayload(json, formattedKey)
            }
        }
    }

    private fun hbasePayload(json: JsonObject, formattedKey: ByteArray): HbasePayload {
        val (timestamp, source) = converter.getLastModifiedTimestamp(json)
        val message = json["message"] as JsonObject
        message["timestamp_created_from"] = source
        val version = converter.getTimestampAsLong(timestamp)
        return HbasePayload(formattedKey, Bytes.toBytes(json.toJsonString()), version)
    }

    private val textUtils = TextUtils()

}
