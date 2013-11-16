package scalabase

import org.hbase.async.{GetRequest, HBaseClient}
import com.stumbleupon.async.Deferred

class HBaseTable(name: Array[Byte], client: HBaseClient)
extends Table[Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte]] {

  //implicit private def deferredToFuture[T](deferred: Deferred[T]): Future[T]

  def get(rowkey: Array[Byte], family: Array[Byte], qualifier: Array[Byte], versions: Int) {
    val request = new GetRequest(name, rowkey)
      .family(family)
      .qualifier(qualifier)
      .maxVersions(versions)

    client.get(request)
  }

  private def retrieve(request: GetRequest) = {
    client.get(request)
  }

}
