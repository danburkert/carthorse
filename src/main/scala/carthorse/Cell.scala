package carthorse

import java.{util => ju}

import org.hbase.async.KeyValue

final case class Cell[R <% Ordered[R], Q <% Ordered[Q]](
    rowkey: R,
    family: String,
    qualifier: Q,
    version: Long,
    value: Array[Byte])
  extends Ordered[Cell[R, Q]] {

  override def equals(any: Any): Boolean = any match {
    case other@Cell(otherRowkey, otherFamily, otherQualifier, otherVersion, otherValue) =>
      (this eq other) || (Cell.equals(rowkey, otherRowkey)
          && family == otherFamily
          && Cell.equals(qualifier, otherQualifier)
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
        ) + Cell.hashCode(qualifier)
      ) + version.hashCode
    ) + ju.Arrays.hashCode(value)

  def compare(other: Cell[R, Q]): Int = {
    val rowkey = this.rowkey.compare(other.rowkey)
    if (rowkey != 0) return rowkey
    val family = this.family compare other.family
    if (family != 0) return family
    val qualifier = this.qualifier.compare(other.qualifier)
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

  def apply[R <% Ordered[R], Q <% Ordered[Q]]
  (decodeRowkey: Array[Byte] => R, decodeQualifier: Array[Byte] => Q)
  (kv: KeyValue): Cell[R, Q] = {
    // TODO: share family String instances
    new Cell(decodeRowkey(kv.key), new String(kv.family, Charset), decodeQualifier(kv.qualifier), kv.timestamp, kv.value)
  }
}
