package carthorse

import org.hbase.async.HBaseClient

//trait HBaseTableLike[+R, +F, +Q, +V, +Repr] extends Table[R, F, Q, V] {
//
//  def bytesToRowKey(bytes: Array[Byte]): R
//  def rowKeyToBytes(rowKey: R): Array[Byte]
//
//  def bytesToFamily(bytes: Array[Byte]): F
//  def familyToBytes(family: F): Array[Byte]
//
//  def bytesToQualifier(bytes: Array[Byte]): Q
//  def qualifierToBytes(qualifier: Q): Array[Byte]
//
//  def bytesToValue(bytes: Array[Byte]): V
//  def valueToBytes(value: V): Array[Byte]
//
//  protected val client: HBaseClient
//
//
//  def column(family: F, qualifier: Q)
//
//}
