package carthorse

import java.{util => ju}

import org.hbase.async.KeyValue
import java.util.Objects

final case class Cell(
    rowkey: RowKey,
    family: String,
    qualifier: Qualifier,
    version: Long,
    value: Array[Byte]) {

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
}

object Cell {
  def apply(kv: KeyValue): Cell =
    Cell(kv.key, new String(kv.family, Charset), kv.qualifier, kv.timestamp, kv.value)
}
