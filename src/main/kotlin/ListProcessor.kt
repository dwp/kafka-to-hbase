import com.beust.klaxon.JsonObject
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import java.io.IOException

class ListProcessor(validator: Validator, private val converter: Converter): BaseProcessor(validator, converter) {

    fun processRecords(hbase: HbaseClient, consumer: KafkaConsumer<ByteArray, ByteArray>,
                       parser: MessageParser,
                       records: ConsumerRecords<ByteArray, ByteArray>) {
        records.partitions().forEach { partition ->
            try {
                val partitionRecords = records.records(partition)
                processList(hbase, parser, partition.topic(), partitionRecords)
                val lastOffset: Long = partitionRecords.get(partitionRecords.size - 1).offset()
                logger.info("Committing offset", "topic", partition.topic(),
                        "partition", "${partition.partition()}", "offset", "$lastOffset")
                consumer.commitSync(mapOf(partition to OffsetAndMetadata(lastOffset + 1)));
            }
            catch (e: IOException) {
                val committed = consumer.committed(partition)
                logger.error("Batch failed, not committing offset, resetting position to last commit", e,
                        "error", e.message ?: "No message",
                        "topic", partition.topic(), "partition", "${partition.partition()}",
                        "committed_offset", "${committed.offset()}")
                consumer.seek(partition, committed.offset())
            }
        }
    }

    @Throws(IOException::class)
    private fun processList(hbase: HbaseClient, parser: MessageParser, topic: String,
                            records: List<ConsumerRecord<ByteArray, ByteArray>>) =
            textUtils.qualifiedTableName(topic)?.let { table ->
                hbase.putList(table, payloads(records, parser))
            }

    private fun payloads(records: List<ConsumerRecord<ByteArray, ByteArray>>, parser: MessageParser): List<HbasePayload> =
            records.mapNotNull { record ->
                recordAsJson(record)?.let { json ->
                    val formattedKey = parser.generateKeyFromRecordBody(json)
                    if (formattedKey.isNotEmpty()) hbasePayload(json, formattedKey) else null
                }
            }

    private fun hbasePayload(json: JsonObject, formattedKey: ByteArray): HbasePayload {
        val (timestamp, source) = converter.getLastModifiedTimestamp(json)
        val message = json["message"] as JsonObject
        message["timestamp_created_from"] = source
        val version = converter.getTimestampAsLong(timestamp)
        return HbasePayload(formattedKey, Bytes.toBytes(json.toJsonString()), version)
    }


    companion object {
        private val textUtils = TextUtils()
        private val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(ListProcessor::class.toString())
    }

}
