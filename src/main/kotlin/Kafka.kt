import org.apache.kafka.clients.producer.KafkaProducer



object kafka {
    val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.props)
}