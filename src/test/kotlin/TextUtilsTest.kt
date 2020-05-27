import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

class TextUtilsTest : StringSpec({


    "agentToDoArchive is coalesced." {
        val actual = TextUtils().coalescedName("agentToDoArchive")
        actual shouldBe "agentToDo"
    }

    "Other archive is not coalesced." {
        val actual = TextUtils().coalescedName("someOtherArchive")
        actual shouldBe "someOtherArchive"
    }

    "Not agentToDoArchive is not coalesced." {
        val actual = TextUtils().coalescedName("calculationParts")
        actual shouldBe "calculationParts"
    }

})
