package scalabase

import org.hbase.async.HBaseClient

class HBase(quorum: Seq[String] = Seq("localhost:2181"), basePath: String = "/hbase") {

  val client = new HBaseClient(quorum.mkString(","), basePath)

  def openTable(name: String): HBaseTable = {
    client.ensureTableExists(name)
    new HBaseTable(name.getBytes("UTF-8"), client)
  }
}
