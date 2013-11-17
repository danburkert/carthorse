package scalabase

import org.hbase.async.HBaseClient
import com.stumbleupon.async.Deferred

class HBase(quorum: Seq[String] = Seq("localhost:2181"), basePath: String = "/hbase") extends AsyncImplicits {


  val client = new HBaseClient(quorum.mkString(","), basePath)

  def ensureTableExists(name: String): RichDeferred[_] = {
    client.ensureTableExists(name)
  }

  def openTable(name: String): HBaseTable = {
    new HBaseTable(name.getBytes("UTF-8"), client)
  }

  def shutdown(): RichDeferred[_] = {
    client.shutdown()
  }
}

object HBase extends AsyncImplicits {
  def main(args: Array[String]) {
    val hbase = new HBase()
    val f: RichDeferred[_] = hbase.ensureTableExists("foo")
    f onComplete println
  }
}
