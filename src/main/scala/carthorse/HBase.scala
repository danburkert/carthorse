package carthorse

import carthorse.async.Deferred
import org.hbase.async.HBaseClient

class HBase(quorum: Seq[String] = Seq("localhost:2181"), basePath: String = "/hbase") {

  val client = new HBaseClient(quorum.mkString(","), basePath)

  def ensureTableExists(name: String): Deferred[_] = client.ensureTableExists(name)

  def openTable[R <% Ordered[R] : OrderedByteable, Q <% Ordered[Q] : OrderedByteable, V <% Ordered[V] : Byteable]
  (name: String, families: String*): HBaseTable[R, Q, String] =
    HBaseTable(client, name, families = families)

  def close(): Deferred[_] = client.shutdown()
}
