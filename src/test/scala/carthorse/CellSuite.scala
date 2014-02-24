package carthorse

import org.scalatest.FunSuite

class CellSuite extends FunSuite {

  val Family = "family"

  // a0..aN are in increasing lexicographic order
  def a0: Array[Byte] = Array()
  def a1: Array[Byte] = Array(1, 2, 3).map(_.toByte)
  def a2: Array[Byte] = Array(1, 2, 3, 4).map(_.toByte)
  def a3: Array[Byte] = Array(2).map(_.toByte)


  def c0 = Cell(a0, Family, a0, 0, a0)
  def c1 = Cell(a0, Family, a0, 0, a0)
  def c2 = Cell(a1, Family, a0, 0, a0)
  def c3 = Cell(a1, Family, a0, 0, a0)


  test("Cell.equals correctly checks equality on byte arrays.") {
    assert(Cell.equals(a0, a0))
    assert(! Cell.equals(a0, a1))
  }

  test("Cell.hashCode correctly hashes byte arrays.") {
    assert(Cell.hashCode(a0) == Cell.hashCode(a0))
  }

  test("Cells with byte array rowkeys have consistent hashing, equality, and comparison.") {
    assert(c0.hashCode() == c0.hashCode())
    assert(c1.hashCode() == c1.hashCode())
    assert(c2.hashCode() == c2.hashCode())
    assert(c3.hashCode() == c3.hashCode())

    assert(c0 == c0)
    assert(c1 == c1)
    assert(c2 == c2)
    assert(c3 == c3)

    assert(c0.compare(c0) == 0)
    assert(c1.compare(c1) == 0)
    assert(c2.compare(c2) == 0)
    assert(c3.compare(c3) == 0)
  }
}
