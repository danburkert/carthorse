package scalabase

import org.hbase.async.{HBaseClient}

class HBaseTable(name: Array[Byte], client: HBaseClient)
extends HBaseTableLike[Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte]]
{




//  def get(rowkey: Array[Byte], family: Array[Byte], qualifier: Array[Byte], versions: Int) {
//    val request = new GetRequest(name, rowkey)
//      .family(family)
//      .qualifier(qualifier)
//      .maxVersions(versions)
//
//    retrieve(request)
//  }
//
//  def put()
//
//  private def retrieve(request: GetRequest): Deferred[Seq[KeyValue]]= {
//    import scala.collection.JavaConverters.asScalaBufferConverter
//    new Deferred(client.get(request)).map(al => al.asScala)
//  }
  def bytesToRowKey(bytes: Array[Byte]): Array[Byte] = ???

  def rowKeyToBytes(rowKey: Array[Byte]): Array[Byte] = ???

  def bytesToFamily(bytes: Array[Byte]): Array[Byte] = ???

  def familyToBytes(family: Array[Byte]): Array[Byte] = ???

  def bytesToQualifier(bytes: Array[Byte]): Array[Byte] = ???

  def qualifierToBytes(qualifier: Array[Byte]): Array[Byte] = ???

  def bytesToValue(bytes: Array[Byte]): Array[Byte] = ???

  def valueToBytes(value: Array[Byte]): Array[Byte] = ???

  def qualifier: Unit = ???
}
