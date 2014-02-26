package carthorse

import java.{util => ju}

import org.hbase.async.KeyValue

final case class Cell[R <% Ordered[R], Q <% Ordered[Q], V : Ordering](
    rowkey: R,
    family: String,
    qualifier: Q,
    value: V)
  extends Ordered[Cell[R, Q, V]] {

  override def equals(any: Any): Boolean = any match {
    case other : Cell[_, _, _] =>
      (this eq other) || (Cell.equals(rowkey, other.rowkey)
          && family == other.family
          && Cell.equals(qualifier, other.qualifier)
          && Cell.equals(value, other.value))
    case _ => false
  }

  override def hashCode(): Int =
    41 * (
      41 * (
        41 * (
          41 + Cell.hashCode(rowkey)
        ) + family.hashCode
      ) + Cell.hashCode(qualifier)
    ) + Cell.hashCode(value)

  override def compare(other: Cell[R, Q, V]): Int = {
    val rowkey = this.rowkey.compare(other.rowkey)
    if (rowkey != 0) return rowkey
    val family = this.family compare other.family
    if (family != 0) return family
    val qualifier = this.qualifier.compare(other.qualifier)
    if (qualifier != 0) return qualifier
    implicitly[Ordering[V]].compare(this.value, other.value)
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

  /**
   * Create a Cell[R, Q, V] from a KeyValue.
   */
  def apply[R <% Ordered[R], Q <% Ordered[Q], V : Ordering]
  (decodeRowkey: Array[Byte] => R, decodeQualifier: Array[Byte] => Q, decodeValue: Array[Byte] => V)
  (kv: KeyValue): Cell[R, Q, V] = {
    // TODO: share family String instances
    new Cell(decodeRowkey(kv.key), new String(kv.family, Charset), decodeQualifier(kv.qualifier), decodeValue(kv.value))
  }

  /**
   * Create a KeyValue from a Cell[R, Q, V].
   */
  def apply[R, Q, V]
  (encodeRowKey: R => Array[Byte], encodeQualifier: Q => Array[Byte], encodeValue: V => Array[Byte])
  (cell: Cell[R, Q, V]): KeyValue =
    // TODO: share family byte array instances
    new KeyValue(
      encodeRowKey(cell.rowkey),
      cell.family.getBytes(Charset),
      encodeQualifier(cell.qualifier),
      encodeValue(cell.value))
}
