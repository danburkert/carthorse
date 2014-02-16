package carthorse

import java.{util => ju}

import com.google.common.primitives.UnsignedBytes
import org.hbase.async.KeyValue

final case class Cell(
    rowkey: Array[Byte],
    family: String,
    qualifier: Array[Byte],
    version: Long,
    value: Array[Byte])
  extends Ordered[Cell] {

  override def equals(any: Any): Boolean = any match {
    case other@Cell(otherRowkey, otherFamily, otherQualifier, otherVersion, otherValue) =>
      (this eq other) ||
        (ju.Arrays.equals(rowkey, otherRowkey)
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
            41 + ju.Arrays.hashCode(rowkey)
          ) + family.hashCode
        ) + ju.Arrays.hashCode(qualifier)
      ) + version.hashCode
    ) + ju.Arrays.hashCode(value)

  def compare(other: Cell): Int = {
    val rowkey = UnsignedBytes.lexicographicalComparator().compare(this.rowkey, other.rowkey)
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
  def apply(kv: KeyValue): Cell =
    // TODO: share family String instances
    new Cell(kv.key, new String(kv.family, Charset), kv.qualifier, kv.timestamp, kv.value)
}
