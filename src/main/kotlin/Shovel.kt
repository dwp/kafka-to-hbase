
import kotlinx.coroutines.*
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.Logger
import java.time.Duration

val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger("ShovelKt")

fun shovelAsync(consumer: KafkaConsumer<ByteArray, ByteArray>, hbase: HbaseClient, pollTimeout: Duration) =
    GlobalScope.async {
        val parser = MessageParser()
        val validator = Validator()
        val converter = Converter()
        val processor = RecordProcessor(validator, converter)
        val offsets = mutableMapOf<String, Long>()
        var batchCount = 0
        val usedPartitions = mutableMapOf<String, MutableSet<Int>>()
        while (isActive) {
            try {
                validateHbaseConnection(hbase)
                logger.debug("Subscribing", "topic_regex", Config.Kafka.topicRegex.pattern(),
                    "metadataRefresh", Config.Kafka.metadataRefresh())
                consumer.subscribe(Config.Kafka.topicRegex)

                logger.info("Polling", "poll_timeout", pollTimeout.toString(), "topic_regex", Config.Kafka.topicRegex.pattern())
                val records = consumer.poll(pollTimeout)

                if (records.count() > 0) {
                    logger.info("Processing records", "record_count", records.count().toString())
                    for (record in records) {
                        processor.processRecord(record, hbase, parser)
                        offsets[record.topic()] = record.offset()
                        val set = if (usedPartitions.containsKey(record.topic())) usedPartitions.get(record.topic()) else mutableSetOf()
                        set?.add(record.partition())
                        usedPartitions[record.topic()] = set!!
                    }
                    logger.info("Commiting offset")
                    consumer.commitSync()
                }

                if (batchCount++ % 50 == 0) {
                    logger.info("Total number of topics", "number_of_topics", offsets.size.toString())
                    offsets.forEach { (topic, offset) ->
                        logger.info("Offset", "topic_name", topic, "offset", offset.toString())
                    }
                    val usedPartitionTuples = mutableListOf<String>()
                    usedPartitions.forEach { (topic, ps) ->
                        usedPartitionTuples.add(topic)
                        usedPartitionTuples.add(ps.joinToString(", "))
                    }
                    logger.info("Partitions this consumer has read from", *usedPartitionTuples.toTypedArray())

                    val allPartitionTuples = mutableListOf<String>()
                    consumer.listTopics().forEach { (topic, partitionInfoList) ->
                        allPartitionTuples.add(topic)
                        allPartitionTuples.add(partitionInfoList.map { it.partition() }.joinToString(", "))
                    }
                    logger.info("All partitions", *allPartitionTuples.toTypedArray())
                }

            } catch (e: Exception) {
                logger.error("Error reading from Kafka or writing to Hbase", e)
                cancel(CancellationException("Error reading from Kafka or writing to Hbase ${e.message}", e))
            }
        }
    }

fun validateHbaseConnection(hbase: HbaseClient){
    val logger = Logger.getLogger("shovel")

    val maxAttempts = Config.Hbase.retryMaxAttempts
    val initialBackoffMillis = Config.Hbase.retryInitialBackoff

    var success = false
    var attempts = 0

    while (!success && attempts < maxAttempts) {
        try {
            HBaseAdmin.checkHBaseAvailable(hbase.connection.configuration)
            success = true
        }
        catch (e: Exception) {
            val delay: Long = if (attempts == 0) initialBackoffMillis
            else (initialBackoffMillis * attempts * 2)
            logger.warn("Failed to connect to Hbase on attempt ${attempts + 1}/$maxAttempts, will retry in $delay ms, if ${attempts + 1} still < $maxAttempts: ${e.message}" )
            Thread.sleep(delay)
        }
        finally {
            attempts++
        }
    }

    if (!success) {
        throw java.io.IOException("Unable to reconnect to Hbase after $attempts attempts")
    }
}
