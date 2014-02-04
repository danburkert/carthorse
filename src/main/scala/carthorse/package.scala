import com.google.common.primitives.UnsignedBytes
import continuum.Discrete
import java.{util => ju}

package object carthorse {

  val Charset = java.nio.charset.Charset.forName("UTF-8")
  type Qualifier = Identifier

  implicit class Identifier(val bytes: Array[Byte]) extends AnyVal {
    override def toString: String = bytes.map("%02X" format _).mkString("Identifier(", ", ", ")")
  }

  /**
   * Provides implicit extensions for `Identifier` values, as well as utilty functions for working
   * with `Identifier`s.
   */
  implicit object Identifier
      extends Ordering[Identifier]
      with Discrete[Identifier] {

    override def next(value: Identifier): Option[Identifier] = {
      val ary = new Array[Byte](value.bytes.length + 1)
      value.bytes.copyToArray(ary)
      Some(ary)
    }

    override def compare(x: Identifier, y: Identifier): Int =
      UnsignedBytes.lexicographicalComparator().compare(x.bytes, y.bytes)

    def equals(x: Identifier, y: Identifier): Boolean = ju.Arrays.equals(x.bytes, y.bytes)

    def hashCode(identifier: Identifier): Int = ju.Arrays.hashCode(identifier.bytes)

    def minimum: Identifier = new Identifier(Array())
  }

  implicit class OrderedBytes(bytes: Array[Byte]) extends Ordered[Array[Byte]] {
    override def compare(other: Array[Byte]): Int =
      UnsignedBytes.lexicographicalComparator().compare(bytes, other)
  }
}
