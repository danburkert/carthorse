package carthorse

import org.hbase.async.KeyValue

case class Cell(
    rowkey: RowKey,
    family: String,
    qualifier: Qualifier,
    version: Long,
    value: Array[Byte])

object Cell {
  def apply(kv: KeyValue): Cell =
    Cell(kv.key, new String(kv.family, Charset), kv.qualifier, kv.timestamp, kv.value)
}
