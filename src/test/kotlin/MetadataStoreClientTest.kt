import com.nhaarman.mockitokotlin2.*
import io.kotlintest.specs.StringSpec
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Timestamp


class MetadataStoreClientTest : StringSpec({

    "works" {
        val statement = mock<PreparedStatement>()

        val connection = mock<Connection> {
            on { prepareStatement(any()) } doReturn statement
        }

        val client = MetadataStoreClient(connection)

        val partition = 1
        val offset = 2L
        val id = "ID"
        val topic = "db.database.collection"
        val record: ConsumerRecord<ByteArray, ByteArray> = mock {
            on { topic() } doReturn topic
            on { partition() } doReturn partition
            on { offset() } doReturn offset
        }

        val lastUpdated = 1L
        client.recordProcessingAttempt(id, record, lastUpdated)

        verify(statement, times(1)).setString(1, id)
        verify(statement, times(1)).setTimestamp(2, Timestamp(lastUpdated))
        verify(statement, times(1)).setString(3, topic)
        verify(statement, times(1)).setInt(4, partition)
        verify(statement, times(1)).setLong(5, offset)
        verify(statement, times(1)).executeUpdate()
    }

})
