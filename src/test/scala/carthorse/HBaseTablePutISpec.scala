package carthorse

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{PropSpec, BeforeAndAfterAll, Matchers}
import java.util.UUID

class HBaseTablePutISpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with BeforeAndAfterAll
  with Generators {

  val TableName = "carthorse.test"
  val Family = "put"

  var hbase: HBase = _
  var table: HBaseTable[UUID, UUID, String] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val quorum = System.getProperty("carthorse.test.quorum", "localhost:2181")
    val basePath = System.getProperty("carthorse.test.basePath", "/hbase")

    hbase = new HBase(quorum.split(','), basePath)
    hbase.ensureTableExists("carthorse.test").result()
    table = hbase.openTable[UUID, UUID, String]("carthorse.test", Family)
    table.deleteRows(table.scan().map(_.rowkey).toSeq:_*)
    table.flush().result()
  }

  override protected def afterAll(): Unit = {
    table.deleteRows(table.scan().map(_.rowkey).toSeq:_*)
    table.flush().result()
    hbase.close()
  }

  property("HBaseTable can put a single cell.") {
    val value = "put-single-cell"
    forAll { (rowkey: UUID, qualifier: UUID) =>
      val cell = Cell[UUID, UUID, String](rowkey, Family, qualifier, value)
      table.put(cell).result()
      table.viewRow(rowkey).scan().toList should equal (List(cell))
    }
  }

  property("HBaseTable can put many cells to a single row.") {
    val value = "put-many-cells"
    forAll { (rowkey: UUID, qualifiers: List[UUID]) =>
      val cells: Seq[Cell[UUID, UUID, String]] =
        qualifiers.map(qualifier => Cell(rowkey, Family, qualifier, value))
      table.put(cells:_*).result()
      val scanned = table.viewRows(rowkey).scan().toList
      for (cell <- cells) {
        scanned should contain (cell)
      }
    }
  }

  property("HBaseTable can put many cells to a many rows.") {
    val value = "put-many-cells-to-many-rows"
    forAll { (rows: List[(UUID, List[UUID])]) =>
      val cells: Seq[Cell[UUID, UUID, String]] =
        rows.flatMap { row: (UUID, List[UUID]) =>
          val rowkey = row._1
          row._2.map((qualifier: UUID) => Cell(rowkey, Family, qualifier, value))
        }
      table.put(cells:_*).result()
      val scanned = table.viewRows(rows.unzip._1:_*).scan().toList
      for (cell <- cells) {
        scanned should contain (cell)
      }
    }
  }
}
