package carthorse

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{PropSpec, BeforeAndAfterAll, Matchers}
import scala.util.Random

class HBaseTablePutISpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with BeforeAndAfterAll
  with Generators {

  val TableName = "carthorse.test"
  val Family = "put"

  var hbase: HBase = _
  var table: HBaseTable[Int, Int, String] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val quorum = System.getProperty("carthorse.test.quorum", "localhost:2181")
    val basePath = System.getProperty("carthorse.test.basePath", "/hbase")

    hbase = new HBase(quorum.split(','), basePath)
    hbase.ensureTableExists("carthorse.test").result()
    table = hbase.openTable[Int, Int, String]("carthorse.test", Family)
  }


  property("HBaseTable can put a single cell.") {
//    forAll { (rowkey: Int, qualifier: Int, value: String) =>
//      val cell = Cell[Int, Int, String](rowkey, Family, qualifier, value)
//      table.put(cell)
//      table.flush().result()
//      val scanned = table.scan().toList
////      table.deleteRows(cell.rowkey)
//      table.flush().result()
//      scanned should equal (List(cell))
//    }
  }

  property("HBaseTable can put many cells to a single table.") {
//    forAll { (ts: Set[(Int, Int, Long, String)]) =>
//      val cells: Seq[Cell[Int, Int, String]] = ts.map(t => Cell(t._1, Family, t._2, t._3, t._4)).toSeq
//      table.put(cells)
//      table.flush().result()
//      val scanned = table.scan().toList
//      table.deleteRows(cells.map(_.rowkey):_*)
//      scanned should equal (cells)
//    }
  }
}
