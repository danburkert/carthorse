package carthorse

import org.scalatest.{Matchers, WordSpec}

class CellSpec extends WordSpec with Matchers {
  "A Cell" when {
    val cell = Cell[RowKey](Array(42.toByte), "fam", Array(13.toByte), 123, Array(0.toByte))
    "copied" should {
      val copy = cell.copy()
      "be equal to its copy" in {
        cell should equal (copy)
      }
    }

    "having the same values as another" should {
      val other = Cell[RowKey](Array(42.toByte), "fam", Array(13.toByte), 123, Array(0.toByte))
      "be equal" in {
        cell should equal (other)
      }
    }
  }
}
