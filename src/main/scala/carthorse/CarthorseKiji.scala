package carthorse

import org.kiji.schema.{KijiTable, KijiURI, Kiji}
import org.kiji.schema.hbase.KijiManagedHBaseTableName
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter

final class CarthorseKiji(
  quorum: Seq[String] = Seq("localhost:2181"),
  basePath: String = "/hbase",
  instance: String = "default") {

  val kijiURI: KijiURI = KijiURI.newBuilder("kiji://.env/").withInstanceName(instance).build()

  val hbase: HBase = new HBase(quorum, basePath)
  val kiji: Kiji = Kiji.Factory.open(kijiURI)

  val kijiTableCache: scala.collection.mutable.Map[String, KijiTable] =
    new ConcurrentHashMap[String, KijiTable].asScala

  def openTable(name: String): CarthorseKijiTable = {
    val hbaseTableName: String = KijiManagedHBaseTableName.getKijiTableName(instance, name).toString

    if (!kijiTableCache.contains(hbaseTableName)) {
      kijiTableCache.put(hbaseTableName, kiji.openTable(name))
    }
    val kijiTable: KijiTable = kijiTableCache.get(hbaseTableName).get

    val hbaseFamilies =
      kijiTable.getLayout.getLocalityGroups.asScala.map(group => group.getId.toString)

    val hbaseTable: HBaseTable = hbase.openTable(hbaseTableName, hbaseFamilies.toSeq:_*)

    new CarthorseKijiTable(hbaseTable, kijiTable)
  }
}
