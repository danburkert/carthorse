package carthorse

import org.hbase.async.HBaseClient
import com.google.common.collect.{Range, RangeSet}

//class HBaseTable(name: Array[Byte], client: HBaseClient)
//extends HBaseTableLike[Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte]] {
//
//  def column(family: Array[Byte], qualifier: Array[Byte]): Unit = ???
//
//  def row(row: Array[Byte]): Table[Array[Byte], Array[Byte], Array[Byte], Array[Byte]] = ???
//
//  def rows(rows: Range[Array[Byte]]): Table[Array[Byte], Array[Byte], Array[Byte],
//      Array[Byte]] = ???
//
//  def rows(rows: RangeSet[Array[Byte]]): Table[Array[Byte], Array[Byte], Array[Byte],
//      Array[Byte]] = ???
//
//  def rows: RangeSet[Array[Byte]] = ???
//
//  def columnFamily(family: Array[Byte]): Table[Array[Byte], Array[Byte], Array[Byte],
//      Array[Byte]] = ???
//
//  def columnFamilies(families: Set[Array[Byte]]): Table[Array[Byte], Array[Byte], Array[Byte],
//      Array[Byte]] = ???
//
//  def columnFamilies: Option[Set[Array[Byte]]] = ???
//
//  def columns(family: Array[Byte], qualifiers: RangeSet[Array[Byte]]): Table[Array[Byte],
//      Array[Byte], Array[Byte], Array[Byte]] = ???
//
//  def columns(columns: Map[Array[Byte], RangeSet[Array[Byte]]]): Table[Array[Byte], Array[Byte],
//      Array[Byte], Array[Byte]] = ???
//
//  def columns: Option[Map[Array[Byte], RangeSet[Array[Byte]]]] = ???
//
//  def version(version: Long): Table[Array[Byte], Array[Byte], Array[Byte], Array[Byte]] = ???
//
//  def versions(versions: RangeSet[Long]): Table[Array[Byte], Array[Byte], Array[Byte],
//      Array[Byte]] = ???
//
//  def versions: Option[RangeSet[Long]] = ???
//
//  def bytesToRowKey(bytes: Array[Byte]): Array[Byte] = ???
//
//  def rowKeyToBytes(rowKey: Array[Byte]): Array[Byte] = ???
//
//  def bytesToFamily(bytes: Array[Byte]): Array[Byte] = ???
//
//  def familyToBytes(family: Array[Byte]): Array[Byte] = ???
//
//  def bytesToQualifier(bytes: Array[Byte]): Array[Byte] = ???
//
//  def qualifierToBytes(qualifier: Array[Byte]): Array[Byte] = ???
//
//  def bytesToValue(bytes: Array[Byte]): Array[Byte] = ???
//
//  def valueToBytes(value: Array[Byte]): Array[Byte] = ???
//
//  def qualifier: Unit = ???
//}
