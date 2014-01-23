package carthorse

import java.{util => ju}

import org.hbase.async.KeyValue
import com.google.common.primitives.UnsignedBytes

final case class Cell(
    rowkey: RowKey,
    family: String,
    qualifier: Qualifier,
    version: Long,
    value: Array[Byte])
  extends Ordered[Cell] {

  override def equals(any: Any): Boolean = any match {
    case other@Cell(otherRowkey, otherFamily, otherQualifier, otherVersion, otherValue) =>
      (this eq other) ||
        (Identifier.equals(rowkey, otherRowkey)
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
            41 + Identifier.hashCode(rowkey)
          ) + family.hashCode
        ) + Identifier.hashCode(qualifier)
      ) + version.hashCode
    ) + ju.Arrays.hashCode(value)

  def compare(other: Cell): Int = {
    val rowkey = Identifier.compare(this.rowkey, other.rowkey)
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
  def apply(kv: KeyValue): Cell =
    Cell(kv.key, new String(kv.family, Charset), kv.qualifier, kv.timestamp, kv.value)
}
