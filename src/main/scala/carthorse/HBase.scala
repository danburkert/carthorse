package carthorse

import org.hbase.async.HBaseClient

import carthorse.async.Deferred

class HBase(quorum: Seq[String] = Seq("localhost:2181"), basePath: String = "/hbase") {

  val client = new HBaseClient(quorum.mkString(","), basePath)

  def ensureTableExists(name: String): Deferred[_] = client.ensureTableExists(name)

  def openTable[R <% Ordered[R] : OrderedByteable](name: String, families: String*): HBaseTable[R] =
    HBaseTable(client, name, families = families)

  def close(): Deferred[_] = client.shutdown()
}
