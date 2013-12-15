package carthorse

import org.hbase.async.HBaseClient

import carthorse.async.Deferred

class HBase(quorum: Seq[String] = Seq("localhost:2181"), basePath: String = "/hbase") {

  val client = new HBaseClient(quorum.mkString(","), basePath)

  def ensureTableExists(name: String): Deferred[_] = client.ensureTableExists(name)

  def openTable(name: String, families: String*): HBaseTable =
    HBaseTable(name, client, families = families)

  def shutdown(): Deferred[_] = client.shutdown()
}
