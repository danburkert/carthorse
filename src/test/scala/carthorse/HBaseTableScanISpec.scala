package carthorse

import scala.collection.immutable.SortedSet

import continuum.{IntervalSet, Interval}
import org.hbase.async.{PutRequest, HBaseClient}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{PropSpec, BeforeAndAfterAll, Matchers}
import scala.collection.mutable

class HBaseTableScanISpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with BeforeAndAfterAll
  with Generators {

  val TableName = "carthorse.test"
  val Family = "scan"

  var hbase: HBase = _
  var table: HBaseTable[Byte, Byte, String] = _

  val rowkeys: mutable.Buffer[String] = mutable.Buffer()

  val cells: Set[Cell[Byte, Byte, String]] =
    (Byte.MinValue to Byte.MaxValue).map { b =>
      val byte = b.toByte
      val version = if (byte >= 0) byte else byte + 256
      Cell(byte, Family, byte, version, byte.toString)
    }.toSet

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val quorum = System.getProperty("carthorse.test.quorum", "localhost:2181")
    val basePath = System.getProperty("carthorse.test.basePath", "/hbase")

    hbase = new HBase(quorum.split(','), basePath)
    hbase.ensureTableExists("carthorse.test").result()
    table = hbase.openTable[Byte, Byte, String]("carthorse.test", Family)

    prepareTable(hbase.client)
  }

  private def prepareTable(client: HBaseClient): Unit = {
    val tableBytes = TableName.getBytes(Charset)
    val familyBytes = Family.getBytes(Charset)

    cells.foreach { cell =>
      client.put(
        new PutRequest(
            tableBytes,
            OrderedByteable.toBytesAsc(cell.rowkey),
            familyBytes,
            OrderedByteable.toBytesAsc(cell.qualifier),
            Byteable.toBytes(cell.value),
            cell.version))
    }
    client.flush().join()
  }

  override protected def afterAll(): Unit = {
    hbase.close().result()
    super.afterAll()
  }

  property("HBaseTable can scan all Cells in a table.") {
    table.scan().toSet should equal (cells)
  }

  property("An HBaseTable scan restricted to rowkeys (-∞, MinValue) should return 0 Cells.") {
    table.viewRows(Interval.lessThan(Byte.MinValue)).scan().length should equal (0)
  }

  property("HBaseTable can scan all Cells in a table restricted by a rowkey interval.") {
    forAll { rowkey: Interval[Byte] =>
      table.viewRows(rowkey).scan().toSet should equal (cells.filter(cell => rowkey(cell.rowkey)))
    }
  }

  property("HBaseTable can scan all Cells in a table restricted by rowkeys.") {
    forAll { rks: List[Byte] =>
      val rowkeys = SortedSet(rks:_*)
      val result = table.viewRows(rks:_*).scan().toSet
      val expected = cells.filter(cell => rowkeys(cell.rowkey))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted by a rowkey interval set.") {
    forAll { rowkeys: IntervalSet[Byte] =>
      val result = table.viewRows(rowkeys).scan().toSet
      val expected = cells.filter(cell => rowkeys(Interval(cell.rowkey)))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a qualifier.") {
    forAll { qualifier: Byte =>
      val result = table.viewColumn(Family, qualifier).scan().toSet
      val expected = cells.filter(cell => cell.qualifier == qualifier)
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to qualifiers.") {
    forAll { qs: List[Byte] =>
      val qualifiers = SortedSet(qs:_*)
      val result = table.viewColumns(Family, qualifiers.toSeq:_*).scan().toSet
      val expected = cells.filter(cell => qualifiers(cell.qualifier))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a qualifier interval.") {
    forAll { qualifiers: Interval[Byte] =>
      val result = table.viewColumns(Family, IntervalSet(qualifiers)).scan().toSet
      val expected = cells.filter(cell => qualifiers(cell.qualifier))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a qualifier interval set.") {
    forAll { qualifiers: IntervalSet[Byte] =>
      val result = table.viewColumns(Family, qualifiers).scan().toSet
      val expected = cells.filter(cell => qualifiers(Interval(cell.qualifier)))
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

  property("An HBaseTable view can be restricted by a rows interval set and a columns interval set.") {
    forAll { (rows: IntervalSet[Byte], qualifiers: IntervalSet[Byte]) =>
      val result = table.viewRows(rows).viewColumns(Family, qualifiers).scan().toSet
      val expected = cells.filter(cell =>
        rows.containsPoint(cell.rowkey) &&
          qualifiers.containsPoint(cell.qualifier))
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by a rows interval set and a versions interval.") {
    forAll { (rows: IntervalSet[Byte], versions: Interval[Long]) =>
      val result = table.viewRows(rows).viewVersions(versions).scan().toSet
      val expected = cells.filter(cell => rows.containsPoint(cell.rowkey) && versions(cell.version))
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by a rows interval set and versions.") {
    forAll { (rows: IntervalSet[Byte], versions: Set[Long]) =>
      val result = table.viewRows(rows).viewVersions(versions.toSeq:_*).scan().toSet
      val expected = cells.filter { cell =>
        rows.containsPoint(cell.rowkey) &&
        versions(cell.version)
      }
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by a qualifier interval set and a versions interval.") {
    forAll { (qualifiers: IntervalSet[Byte], versions: Interval[Long]) =>
      val result = table.viewColumns(Family, qualifiers).viewVersions(versions).scan().toSet
      val expected = cells.filter { cell =>
        qualifiers.containsPoint(cell.qualifier) &&
        versions(cell.version)
      }
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by a qualifier interval set and versions.") {
    forAll { (qualifiers: IntervalSet[Byte], versions: Set[Long]) =>
      val result = table.viewColumns(Family, qualifiers).viewVersions(versions.toSeq:_*).scan().toSet
      val expected = cells.filter { cell =>
        qualifiers.containsPoint(cell.qualifier) &&
          versions(cell.version)
      }
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by rowkey, qualifier, and version interval.") {
    forAll { (rows: IntervalSet[Byte], qualifiers: IntervalSet[Byte], versions: Interval[Long]) =>
      val result = table.viewRows(rows).viewColumns(Family, qualifiers).viewVersions(versions).scan().toSet
      val expected = cells.filter { cell =>
        rows.containsPoint(cell.rowkey) &&
        qualifiers.containsPoint(cell.qualifier) &&
        versions(cell.version)
      }
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by rowkey, qualifier, and versions.") {
    forAll { (rows: IntervalSet[Byte], qualifiers: IntervalSet[Byte], versions: Set[Long]) =>
      val result = table.viewRows(rows).viewColumns(Family, qualifiers).viewVersions(versions.toSeq:_*).scan().toSet
      val expected = cells.filter(cell =>
        rows.containsPoint(cell.rowkey) &&
          qualifiers.containsPoint(cell.qualifier) &&
          versions(cell.version))
      result should equal (expected)
    }
  }
}
