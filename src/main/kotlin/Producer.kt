import org.apache.kafka.clients.producer.KafkaProducer


object Producer {
    private var INSTANCE: KafkaProducer<ByteArray, ByteArray>? = null
       fun getInstance () : KafkaProducer<ByteArray, ByteArray>? {
            if (INSTANCE == null) {
                INSTANCE = KafkaProducer(Config.Kafka.producerProps)
            }

            return INSTANCE!!
        }
}
