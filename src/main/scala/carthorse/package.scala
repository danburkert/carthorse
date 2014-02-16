import com.google.common.primitives.UnsignedBytes

package object carthorse {

  val Charset = java.nio.charset.Charset.forName("UTF-8")

  implicit class RichBytes(val bytes: Array[Byte]) extends AnyVal with Ordered[Array[Byte]] {
    override def compare(other: Array[Byte]): Int =
      UnsignedBytes.lexicographicalComparator().compare(bytes, other)

    override def toString: String = bytes.map("%02X" format _).mkString("RichBytes(", ", ", ")")
  }
}
