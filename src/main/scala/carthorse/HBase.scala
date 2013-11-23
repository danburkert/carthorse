package carthorse

import org.hbase.async.HBaseClient
import scalabase.async.Deferred

//class HBase(quorum: Seq[String] = Seq("localhost:2181"), basePath: String = "/hbase") {
//
//  val client = new HBaseClient(quorum.mkString(","), basePath)
//
//  def ensureTableExists(name: String): Deferred[_] = {
//    client.ensureTableExists(name)
//  }
//
//  def openTable(name: String): HBaseTable = {
//    new HBaseTable(name.getBytes("UTF-8"), client)
//  }
//
//  def shutdown(): Deferred[_] = {
//    client.shutdown()
//  }
//}
//
//object HBase {
//  def main(args: Array[String]) {
//    val hbase = new HBase()
//    val f: Deferred[_] = hbase.ensureTableExists("foo")
//    f onComplete println
//  }
//}
