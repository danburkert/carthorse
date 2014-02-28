package carthorse

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import continuum.Interval
import org.kiji.schema.avro.TableLayoutDesc
import scala.collection.immutable.IntervalSet

case class KijiCluster(quorum: Seq[String] = Seq("localhost:2181")) {

  val zookeeper =
    CuratorFrameworkFactory.newClient(quorum.mkString(","), new ExponentialBackoffRetry(1000, 3))
  zookeeper.start()

  val hbase = new HBase(quorum)

//
//  val layoutTable = new HBaseTable[String, String, TableLayoutDesc](
//    hbase.client,
//    "kiji.default.meta",
//    columns = Map("layout" -> IntervalSet(Interval("layout".getBytes(Charset)))),
//    encodeRowkey = Byteable.toBytes[String],
//    decodeRowkey = Byteable.fromBytes[String],
//    encodeQualifier = Byteable.toBytes[String],
//    decodeQualifier = Byteable.fromBytes[String],
//    encodeValue = Byteable.toBytes[TableLayoutDesc],
//    decodeValue = Byteable.fromBytes[TableLayoutDesc]
//  )

}
