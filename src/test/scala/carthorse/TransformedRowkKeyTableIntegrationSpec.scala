package carthorse

import org.hbase.async.{PutRequest, HBaseClient}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{PropSpec, BeforeAndAfterAll, Matchers}

class TransformedRowkKeyTableIntegrationSpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with BeforeAndAfterAll
  with Generators {

    val TableName = "carthorse.test"
    val Family = "b"

    var hbase: HBase = _
    var table: HBaseTable[RowKey] = _

    val cells: Set[Cell[Byte]] = (for (byte <- Byte.MinValue to Byte.MaxValue) yield {
      val bytes = Array(byte.toByte)
      val version = if (byte >= 0) byte else byte + 256
      Cell[Byte](byte.toByte, Family, bytes, version, bytes)
    }).toSet

    val encode: (Byte => RowKey) = OrderedByteable.toBytesAsc[Byte] _
    val decode: (RowKey => Byte) = OrderedByteable.fromBytesAsc[Byte] _

    override protected def beforeAll(): Unit = {
      val quorum = System.getProperty("carthorse.test.quorum", "localhost:2181")
      val basePath = System.getProperty("carthorse.test.basePath", "/hbase")

      hbase = new HBase(quorum.split(','), basePath)
      hbase.ensureTableExists("carthorse.test").join()
      table = hbase.openTable("carthorse.test", Family)

      prepareTable(hbase.client)

      super.beforeAll()
    }

    override protected def afterAll(): Unit = {
      hbase.close().join()
      super.afterAll()
    }

    private def prepareTable(client: HBaseClient): Unit = {
      val tableBytes = TableName.getBytes(Charset)
      val familyBytes = Family.getBytes(Charset)

      cells.foreach { cell =>
        client.put(
          new PutRequest(
            tableBytes,
            encode(cell.rowkey).bytes,
            familyBytes,
            cell.qualifier.bytes,
            cell.value.bytes,
            cell.version))
      }
      client.flush().join()
    }

}
