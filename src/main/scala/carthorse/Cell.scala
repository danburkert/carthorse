package carthorse

import java.{util => ju}

import org.hbase.async.KeyValue
import com.google.common.primitives.UnsignedBytes

final case class Cell[R <% Ordered[R]](
    rowkey: R,
    family: String,
    qualifier: Qualifier,
    version: Long,
    value: Array[Byte])
  extends Ordered[Cell[R]] {

  override def equals(any: Any): Boolean = any match {
    case other@Cell(otherRowkey, otherFamily, otherQualifier, otherVersion, otherValue) =>
      (this eq other) ||
        (Cell.equals(rowkey, otherRowkey)
          && family == otherFamily
          && Identifier.equals(qualifier, otherQualifier)
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
        ) + Identifier.hashCode(qualifier)
      ) + version.hashCode
    ) + ju.Arrays.hashCode(value)

  def compare(other: Cell[R]): Int = {
    val rowkey = this.rowkey compare other.rowkey
    if (rowkey != 0) return rowkey
    val family = this.family compare other.family
    if (family != 0) return family
    val qualifier = Identifier.compare(this.qualifier, other.qualifier)
    if (qualifier != 0) return qualifier
    val version = this.version compare other.version
    if (version != 0) return version

    UnsignedBytes.lexicographicalComparator().compare(this.value, other.value)
  }
}

object Cell {
  def apply[R <% Ordered[R]](r: (RowKey => R))(kv: KeyValue): Cell[R] =
    Cell(r(kv.key), new String(kv.family, Charset), kv.qualifier, kv.timestamp, kv.value)

  def equals(a: Any, b: Any): Boolean = (a, b) match {
    case (x: Identifier, y: Identifier) => Identifier.equals(x, y)
    case _ => a == b
  }

  def hashCode(a: Any): Int = a match {
    case x: Identifier => Identifier.hashCode(x)
    case _ => a.hashCode
  }
}
