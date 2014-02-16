package carthorse

import java.{util => ju}

import scala.collection.immutable.SortedSet

import continuum.{IntervalSet, Interval}
import org.hbase.async.{PutRequest, HBaseClient}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{PropSpec, BeforeAndAfterAll, Matchers}

class HBaseTableIntegrationSpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with BeforeAndAfterAll
  with Generators {

  val TableName = "carthorse.test"
  val Families = List("a")

  var hbase: HBase = _
  var table: HBaseTable[Byte] = _

  val cells: Set[Cell] = (for (b <- Byte.MinValue to Byte.MaxValue; byte = b.toByte) yield {
    val bytes = Array(byte)
    val version = if (byte >= 0) byte else byte + 256
    Cell(OrderedByteable.toBytesAsc(byte), Families(0), OrderedByteable.toBytesAsc(byte), version, bytes)
  }).toSet

  override protected def beforeAll(): Unit = {
    val quorum = System.getProperty("carthorse.test.quorum", "localhost:2181")
    val basePath = System.getProperty("carthorse.test.basePath", "/hbase")

    hbase = new HBase(quorum.split(','), basePath)
    hbase.ensureTableExists("carthorse.test").join()
    table = hbase.openTable[Byte]("carthorse.test", Families: _*)

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
          cell.rowkey,
          familyBytes,
          cell.qualifier,
          cell.value,
          cell.version))
    }
    client.flush().join()
  }

  property("HBaseTable can scan all Cells in a table.") {
    table.scan().toSet should equal (cells)
  }

  property("An HBaseTable scan restricted to rowkeys (-∞, MinValue) should return 0 Cells.") {
    table.viewRows(Interval.lessThan(Byte.MinValue)).scan().length should equal (0)
  }

  property("HBaseTable can scan all Cells in a table restricted by a rowkey interval.") {
    forAll { rowkey: Interval[Byte] =>
      table.viewRows(rowkey).scan().toSet should equal (cells.filter(cell => rowkey(OrderedByteable.fromBytesAsc[Byte](cell.rowkey))))
    }
  }

  property("HBaseTable can scan all Cells in a table restricted by rowkeys.") {
    forAll { rks: List[Byte] =>
      val rowkeys = SortedSet(rks:_*)
      val result = table.viewRows(rks:_*).scan().toSet
      val expected = cells.filter(cell => rowkeys(OrderedByteable.fromBytesAsc[Byte](cell.rowkey)))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted by a rowkey interval set.") {
    forAll { rowkeys: IntervalSet[Byte] =>
      val result = table.viewRows(rowkeys).scan().toSet
      val expected = cells.filter(cell => rowkeys(Interval(OrderedByteable.fromBytesAsc[Byte](cell.rowkey))))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a qualifier.") {
    forAll { qualifier: Byte =>
      val result = table.viewColumn(Families(0), qualifier).scan().toSet
      val expected = cells.filter(cell => OrderedByteable.fromBytesAsc[Byte](cell.qualifier) == qualifier)
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to qualifiers.") {
    forAll { qs: List[Byte] =>
      val qualifiers = SortedSet(qs:_*)
      val result = table.viewColumns(Families(0), qualifiers.toSeq:_*).scan().toSet
      val expected = cells.filter(cell => qualifiers(OrderedByteable.fromBytesAsc[Byte](cell.qualifier)))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a qualifier interval.") {
    forAll { qualifiers: Interval[Byte] =>
      val result = table.viewColumns(Families(0), IntervalSet(qualifiers)).scan().toSet
      val expected = cells.filter(cell => qualifiers(OrderedByteable.fromBytesAsc[Byte](cell.qualifier)))
      result should equal (expected)
    }
  }

  property("HBaseTable can scan all Cells in a table restricted to a qualifier interval set.") {
    forAll { qualifiers: IntervalSet[Byte] =>
      val result = table.viewColumns(Families(0), qualifiers).scan().toSet
      val expected = cells.filter(cell => qualifiers(Interval(OrderedByteable.fromBytesAsc[Byte](cell.qualifier))))
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
      val result = table.viewRows(rows).viewColumns(Families(0), qualifiers).scan().toSet
      val expected = cells.filter(cell =>
        rows.containsPoint(OrderedByteable.fromBytesAsc[Byte](cell.rowkey)) &&
          qualifiers.containsPoint(OrderedByteable.fromBytesAsc[Byte](cell.qualifier)))
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by a rows interval set and a versions interval.") {
    forAll { (rows: IntervalSet[Byte], versions: Interval[Long]) =>
      val result = table.viewRows(rows).viewVersions(versions).scan().toSet
      val expected = cells.filter(cell => rows.containsPoint(OrderedByteable.fromBytesAsc[Byte](cell.rowkey)) && versions(cell.version))
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by a rows interval set and versions.") {
    forAll { (rows: IntervalSet[Byte], versions: Set[Long]) =>
      val result = table.viewRows(rows).viewVersions(versions.toSeq:_*).scan().toSet
      val expected = cells.filter { cell =>
        rows.containsPoint(OrderedByteable.fromBytesAsc[Byte](cell.rowkey)) &&
        versions(cell.version)
      }
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by a qualifier interval set and a versions interval.") {
    forAll { (qualifiers: IntervalSet[Byte], versions: Interval[Long]) =>
      val result = table.viewColumns(Families(0), qualifiers).viewVersions(versions).scan().toSet
      val expected = cells.filter { cell =>
        qualifiers.containsPoint(OrderedByteable.fromBytesAsc[Byte](cell.qualifier)) &&
        versions(cell.version)
      }
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by a qualifier interval set and versions.") {
    forAll { (qualifiers: IntervalSet[Byte], versions: Set[Long]) =>
      val result = table.viewColumns(Families(0), qualifiers).viewVersions(versions.toSeq:_*).scan().toSet
      val expected = cells.filter { cell =>
        qualifiers.containsPoint(OrderedByteable.fromBytesAsc[Byte](cell.qualifier)) &&
          versions(cell.version)
      }
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by rowkey, qualifier, and version interval.") {
    forAll { (rows: IntervalSet[Byte], qualifiers: IntervalSet[Byte], versions: Interval[Long]) =>
      val result = table.viewRows(rows).viewColumns(Families(0), qualifiers).viewVersions(versions).scan().toSet
      val expected = cells.filter { cell =>
        rows.containsPoint(OrderedByteable.fromBytesAsc[Byte](cell.rowkey)) &&
        qualifiers.containsPoint(OrderedByteable.fromBytesAsc[Byte](cell.qualifier)) &&
        versions(cell.version)
      }
      result should equal (expected)
    }
  }

  property("An HBaseTable view can be restricted by rowkey, qualifier, and versions.") {
    forAll { (rows: IntervalSet[Byte], qualifiers: IntervalSet[Byte], versions: Set[Long]) =>
      val result = table.viewRows(rows).viewColumns(Families(0), qualifiers).viewVersions(versions.toSeq:_*).scan().toSet
      val expected = cells.filter(cell =>
        rows.containsPoint(OrderedByteable.fromBytesAsc[Byte](cell.rowkey)) &&
          qualifiers.containsPoint(OrderedByteable.fromBytesAsc[Byte](cell.qualifier)) &&
          versions(cell.version))
      result should equal (expected)
    }
  }
}
