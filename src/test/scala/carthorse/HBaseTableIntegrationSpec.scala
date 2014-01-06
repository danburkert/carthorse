package carthorse

import org.hbase.async.{PutRequest, HBaseClient}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{PropSpec, BeforeAndAfterAll, Matchers}
import continuum.{IntervalSet, Interval}
import scala.collection.immutable.SortedSet

class HBaseTableIntegrationSpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with BeforeAndAfterAll
  with Generators {

  val TableName = "carthorse.test"
  val Families = List("a")

  var hbase: HBase = _
  var table: HBaseTable = _

  val cells: Set[Cell] = (for (byte <- Byte.MinValue to Byte.MaxValue) yield {
    val bytes = Array(byte.toByte)
    val version = if (byte >= 0) byte else byte + 256
    Cell(bytes, Families(0), bytes, version, bytes)
  }).toSet

  override protected def beforeAll(): Unit = {
    val quorum = System.getProperty("carthorse.test.quorum", "localhost:2181")
    val basePath = System.getProperty("carthorse.test.basePath", "/hbase")

    hbase = new HBase(quorum.split(','), basePath)
    hbase.ensureTableExists("carthorse.test").join()
    table = hbase.openTable("carthorse.test", Families: _*)

    prepareTable(hbase.client)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    hbase.close().join()
    super.afterAll()
  }

  private def prepareTable(client: HBaseClient): Unit = {
    val tableBytes = TableName.getBytes(Charset)
    val familyBytes = Families(0).getBytes(Charset)

    cells.foreach { cell =>
      client.put(
        new PutRequest(
          tableBytes,
          cell.rowkey.bytes,
          familyBytes,
          cell.qualifier.bytes,
          cell.value.bytes,
          cell.version))
    }
    client.flush().join()
  }

  property("HBaseTable can scan all Cells in a table.") {
    table.scan().toSet should equal (cells)
  }

  property("An HBaseTable scan restricted to rowkeys (-∞, Identifier()) should return 0 Cells.") {
    table.viewRows(Interval.lessThan(new RowKey(Array()))).scan().length should equal (0)
  }

  property("HBaseTable can scan all Cells in a table restricted by a rowkey interval.") {
    forAll { rowkey: Interval[RowKey] =>
      table.viewRows(rowkey).scan().toSet should equal (cells.filter(cell => rowkey(cell.rowkey)))
    }
  }

  property("HBaseTable can scan all Cells in a table restricted by rowkeys.") {
    forAll { rks: List[RowKey] =>
      val rowkeys = SortedSet(rks:_*)
      val result = table.viewRows(rks:_*).scan().toSet
      val expected = cells.filter(cell => rowkeys(cell.rowkey))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted by a rowkey interval set.") {
    forAll { rowkeys: IntervalSet[RowKey] =>
      val result = table.viewRows(rowkeys).scan().toSet
      val expected = cells.filter(cell => rowkeys(Interval.point(cell.rowkey)))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a qualifier.") {
    forAll { qualifier: Qualifier =>
      val result = table.viewColumn(Families(0), qualifier).scan().toSet
      val expected = cells.filter(cell => Identifier.equals(cell.qualifier, qualifier))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to qualifiers.") {
    forAll { qs: List[Qualifier] =>
      val qualifiers = SortedSet(qs:_*)
      val result = table.viewColumns(Families(0), qualifiers.toSeq:_*).scan().toSet
      val expected = cells.filter(cell => qualifiers(cell.qualifier))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a qualifier interval.") {
    forAll { qualifiers: Interval[Qualifier] =>
      val result = table.viewColumns(Families(0), IntervalSet(qualifiers)).scan().toSet
      val expected = cells.filter(cell => qualifiers(cell.qualifier))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a qualifier interval set.") {
    forAll { qualifiers: IntervalSet[Qualifier] =>
      val result = table.viewColumns(Families(0), qualifiers).scan().toSet
      val expected = cells.filter(cell => qualifiers(Interval.point(cell.qualifier)))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a version.") {
    forAll { version: Long =>
      whenever(version >= 0) {
        val result = table.viewVersion(version).scan().toSet
        val expected = cells.filter(cell => cell.version == version)
        result should equal (expected)
      }
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to versions.") {
    forAll { longs: List[Long] =>
      val versions = longs.toSet
      val result = table.viewVersions(longs:_*).scan().toSet
      val expected = cells.filter(cell => versions(cell.version))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a version interval.") {
    forAll { versions: Interval[Long] =>
      val result = table.viewVersions(versions).scan().toSet
      val expected = cells.filter(cell => versions(cell.version))
      result should equal (expected)
    }
  }

}
