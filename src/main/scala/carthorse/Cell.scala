package carthorse

import java.{util => ju}

import com.google.common.primitives.UnsignedBytes
import org.hbase.async.KeyValue

final case class Cell[R <% Ordered[R]](
    rowkey: R,
    family: String,
    qualifier: Array[Byte],
    version: Long,
    value: Array[Byte])
  extends Ordered[Cell[R]] {

  override def equals(any: Any): Boolean = any match {
    case other@Cell(otherRowkey, otherFamily, otherQualifier, otherVersion, otherValue) =>
      (this eq other) ||
        (rowkey.compareTo(otherRowkey.asInstanceOf[R]) == 0
          && family == otherFamily
          && ju.Arrays.equals(qualifier, otherQualifier)
          && version == otherVersion
          && ju.Arrays.equals(value, otherValue))
    case _ => false
  }

  override def hashCode(): Int =
    41 * (
      41 * (
        41 * (
          41 * (
            41 + Cell.hashCode(rowkey)
          ) + family.hashCode
        ) + ju.Arrays.hashCode(qualifier)
      ) + version.hashCode
    ) + ju.Arrays.hashCode(value)

  def compare(other: Cell[R]): Int = {
    val rowkey = this.rowkey.compare(other.rowkey)
    if (rowkey != 0) return rowkey
    val family = this.family compare other.family
    if (family != 0) return family
    val qualifier = UnsignedBytes.lexicographicalComparator().compare(this.qualifier, other.qualifier)
    if (qualifier != 0) return qualifier
    val version = this.version compare other.version
    if (version != 0) return version
    this.value compare other.value
  }
}

object Cell {

  /**
   * Checks if the arguments are equal. Delegates to the underlying .equals of `a` unless `a` and
   * `b` are byte arrays, in which case a proper equality check is done.
   */
  def equals(a: Any, b: Any): Boolean = (a, b) match {
    case (a: Array[Byte], b: Array[Byte]) => ju.Arrays.equals(a, b)
    case _ => a equals b
  }

  /**
   * Returns the hashCode of the provided argument. Delegates to the underlying .hashCode of the
   * object unless it is a byte array, in which case a proper hash code is returned.
   */
  def hashCode(a: Any): Int = a match {
    case (a: Array[Byte]) => ju.Arrays.hashCode(a)
    case _ => a.hashCode()
  }

  def apply[R <% Ordered[R]](decodeRowkey: Array[Byte] => R)(kv: KeyValue): Cell[R] = {
    // TODO: share family String instances
    new Cell(decodeRowkey(kv.key), new String(kv.family, Charset), kv.qualifier, kv.timestamp, kv.value)
  }
}
