import com.google.common.primitives.UnsignedBytes
import continuum.Discrete

package object carthorse {

  val Charset = java.nio.charset.Charset.forName("UTF-8")
  type RowKey = Identifier
  type Qualifier = Identifier

  implicit class Identifier(val bytes: Array[Byte]) extends AnyVal

  implicit object Identifier extends Ordering[Identifier] with Discrete[Identifier] {
    def next(value: Identifier): Option[Identifier] = {
      val ary = new Array[Byte](value.bytes.length + 1)
      value.bytes.copyToArray(ary)
      Some(ary)
    }

    def compare(x: Identifier, y: Identifier): Int =
      UnsignedBytes.lexicographicalComparator().compare(x.bytes, y.bytes)
  }
}
