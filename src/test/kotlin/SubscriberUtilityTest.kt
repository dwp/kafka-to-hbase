
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import kotlin.time.ExperimentalTime

@ExperimentalTime
class SubscriberUtilityTest : StringSpec() {

    init {
        "Re-subscribes if new topics found" {
            val subscription = setOf("db.database.collection1", "db.database.collection2")
            val topics = listOf("db.database.collection1", "db.database.collection2", "db.database.collection3")
            val consumer = kafkaConsumer<ByteArray, ByteArray>(subscription, topics)
            val includesRegex = Regex("""db\.\w+\.\w+""")
            SubscriberUtility.subscribe(consumer, includesRegex)
            verify(consumer, times(1)).subscription()
            verify(consumer, times(1)).listTopics()
            verify(consumer, times(1))
                    .subscribe(listOf("db.database.collection1", "db.database.collection2", "db.database.collection3"))
            verifyNoMoreInteractions(consumer)
        }

        "Does not re-subscribe if no new topics" {
            val subscription = setOf("db.database.collection1", "db.database.collection2")
            val topics = listOf("db.database.collection1", "db.database.collection2")
            val consumer = kafkaConsumer<ByteArray, ByteArray>(subscription, topics)

            val includesRegex = Regex("""db\.\w+\.\w+""")
            SubscriberUtility.subscribe(consumer, includesRegex)
            verify(consumer, times(1)).subscription()
            verify(consumer, times(1)).listTopics()
            verify(consumer, times(0)).subscribe(any<List<String>>())
            verifyNoMoreInteractions(consumer)
        }

        "Excludes topics" {
            val subscription = setOf("db.include.collection1", "db.include.collection2")
            val topics = listOf("db.include.collection1", "db.include.collection2",
                    "db.include.collection3", "db.exclude.collection3")
            val consumer = kafkaConsumer<ByteArray, ByteArray>(subscription, topics)
            val includesRegex = Regex("""db\.\w+\.\w+""")
            val excludesRegex = Regex("""db\.exclude\.\w+""")
            SubscriberUtility.subscribe(consumer, includesRegex, excludesRegex)
            verify(consumer, times(1)).subscription()
            verify(consumer, times(1)).listTopics()
            verify(consumer, times(1))
                    .subscribe(listOf("db.include.collection1", "db.include.collection2", "db.include.collection3"))
            verifyNoMoreInteractions(consumer)
        }

        "Retries until subscribed" {
            val subscription = setOf<String>()
            val topics = arrayOf(listOf(), listOf(), listOf(),
                    listOf("db.include.collection1", "db.include.collection2"))
            val consumer = kafkaConsumer<ByteArray, ByteArray>(subscription, *topics)

            val includesRegex = Regex("""db\.\w+\.\w+""")
            SubscriberUtility.subscribe(consumer, includesRegex)
            verify(consumer, times(topics.size)).subscription()
            verify(consumer, times(topics.size)).listTopics()
            verify(consumer, times(1)).subscribe(any<List<String>>())
            verifyNoMoreInteractions(consumer)
        }
    }

    private fun <K, V> kafkaConsumer(subscription: Set<String>, topics: List<String>): KafkaConsumer<K, V> =
            mock {
                on { subscription() } doReturn subscription
                on { listTopics() } doReturn topics.zip((0 .. topics.size).map { listOf(mock<PartitionInfo>()) }).toMap()
            }

    private fun <K, V> kafkaConsumer(subscription: Set<String>, vararg topics: List<String>): KafkaConsumer<K, V> =
            mock {
                on { subscription() } doReturn subscription
                on {
                    listTopics()
                } doReturnConsecutively topics.map { it.zip((0 .. topics.size).map { listOf(mock<PartitionInfo>()) }).toMap()}
            }
}

