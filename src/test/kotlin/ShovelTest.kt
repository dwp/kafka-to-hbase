import io.kotlintest.specs.StringSpec
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue

class ShovelTest : StringSpec() {

    init {
        "batchCount is a multiple of reportFrequency" {
            val batchCount = 100
            val isMultiple = batchCountIsMultipleOfReportFrequency(batchCount)

            assertTrue(isMultiple)
        }

        "batchCount is not a multiple of reportFrequency" {
            val batchCount = 101
            val isMultiple = batchCountIsMultipleOfReportFrequency(batchCount)

            assertFalse(isMultiple)
        }
    }
}
