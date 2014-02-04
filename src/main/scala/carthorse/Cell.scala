package carthorse

import org.hbase.async.KeyValue

final case class Cell[R <% OrderedByteable[R]](
    rowkey: R,
    family: String,
    qualifier: Qualifier,
    version: Long,
    value: Array[Byte])

object Cell {
  def apply[R <% OrderedByteable[R] : OrderedByteable](kv: KeyValue): Cell[R] =
    new Cell[R](OrderedByteable.fromBytesAsc(kv.key), new String(kv.family, Charset), kv.qualifier, kv.timestamp, kv.value)
}
