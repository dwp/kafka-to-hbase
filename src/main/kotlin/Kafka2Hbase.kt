import org.apache.kafka.clients.consumer.KafkaConsumer
import sun.misc.Signal
import java.util.logging.Logger

suspend fun main() {
    configureLogging()
    val log = Logger.getLogger("main")

    // Connect to Hbase and create the topic tables
    val hbase = HbaseClient.connect()
    hbase.migrate()

    // Create a Kafka consumer
    val kafka = KafkaConsumer<ByteArray, ByteArray>(Config.Kafka.props)
    log.info("Will subscribe to topics with regex '%s'".format(Config.Kafka.topicRegex.pattern()))
    log.info("Will refresh metadata every %s ms".format(Config.Kafka.props.getProperty("metadata.max.age.ms")))

    // Read as many messages as possible until a signal is received
    val job = shovelAsync(kafka, hbase, Config.Kafka.pollTimeout)

    // Handle signals gracefully and wait for completion
    Signal.handle(Signal("INT")) { job.cancel() }
    Signal.handle(Signal("TERM")) { job.cancel() }
    job.await()

    hbase.close()
    kafka.close()
}