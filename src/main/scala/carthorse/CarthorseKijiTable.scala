package carthorse

import org.kiji.schema._
import continuum.Interval
import scala.collection.immutable.IntervalSet
import carthorse.EntityIdImplicits._
import org.kiji.schema.hbase.HBaseColumnName
import scala.collection.mutable
import com.google.common.collect.{Maps, HashBasedTable, Table}
import org.kiji.schema.layout.impl.{CellDecoderProvider, ColumnNameTranslator}
import org.kiji.schema.layout.{CellSpec, KijiTableLayout}
import scala.Some

case class CarthorseKijiTable(hbaseTable: HBaseTable, kijiTable: KijiTable) {

  lazy val layout: KijiTableLayout = kijiTable.getLayout
  lazy val entityIdFactory: EntityIdFactory = EntityIdFactory.getFactory(layout)
  lazy val columnNameTranslator: ColumnNameTranslator = new ColumnNameTranslator(layout)
  lazy val cellDecoderProvider: CellDecoderProvider =
    new CellDecoderProvider(
      layout,
      kijiTable.getKiji.getSchemaTable,
      GenericCellDecoderFactory.get(),
      Maps.newTreeMap[KijiColumnName, CellSpec]())

  def viewRow(entityId: EntityId): CarthorseKijiTable = viewRows(entityId)

  def viewRows(rows: EntityId*): CarthorseKijiTable =
    copy(hbaseTable = hbaseTable.viewRows(rows.map(entityIdToRowKey):_*))

  def viewRows(rows: Interval[EntityId]): CarthorseKijiTable =
    copy(hbaseTable = hbaseTable.viewRows(rows.map(entityIdToRowKey)))

  def viewRows(rows: IntervalSet[EntityId]): CarthorseKijiTable = {
    val set = rows.map{interval: Interval[EntityId] => interval map entityIdToRowKey }
    copy(hbaseTable = hbaseTable viewRows set)
  }

  private def translateFamily(family: String): String =
    columnNameTranslator.toHBaseColumnName(new KijiColumnName(family, "")).getFamilyAsString

  private def translateQualifier(qualifier: String): Qualifier =
    columnNameTranslator.toHBaseColumnName(new KijiColumnName("", qualifier)).getQualifier

  def viewFamily(family: String) = viewFamilies(family)

  def viewFamilies(families: String*) =
    copy(hbaseTable = hbaseTable.viewFamilies(families.map(translateFamily):_*))

  def viewColumn(family: String, qualifier: String): CarthorseKijiTable = viewColumns(family, qualifier)

  def viewColumns(family: String, qualifiers: String*): CarthorseKijiTable =
    viewColumns(family, IntervalSet(qualifiers.map(Interval(_)):_*))

  def viewColumns(family: String, qualifiers: IntervalSet[String]): CarthorseKijiTable =
    viewColumns(Map(family -> qualifiers))

  def viewColumns(columns: Map[String, IntervalSet[String]]): CarthorseKijiTable = {
    val translated = columns.map { case (family, qualifiers) =>
      translateFamily(family) -> qualifiers.map(_.map(translateQualifier))
    }
    copy(hbaseTable = hbaseTable viewColumns translated)
  }

  def viewVersion(version: Long): CarthorseKijiTable = viewVersions(version)

  def viewVersions(versions: Long*): CarthorseKijiTable =
    copy(hbaseTable = hbaseTable.viewVersions(versions:_*))

  def viewVersions(versions: Interval[Long]): CarthorseKijiTable =
    copy(hbaseTable = hbaseTable viewVersions versions)

  def maxVersions(maxVersions: Option[Int]): CarthorseKijiTable =
    copy(hbaseTable = hbaseTable maxVersions maxVersions)

  def maxCells(maxCells: Option[Int]): CarthorseKijiTable =
    copy(hbaseTable = hbaseTable maxCells maxCells)

  def maxRows(maxRows: Option[Int]): CarthorseKijiTable =
    copy(hbaseTable = hbaseTable maxRows maxRows)

  def maxBytes(maxBytes: Option[Long]): CarthorseKijiTable =
    copy(hbaseTable = hbaseTable maxBytes maxBytes)

  def populateBlockCache(populateBlockCache: Boolean): CarthorseKijiTable =
    copy(hbaseTable = hbaseTable populateBlockCache populateBlockCache)

  def isEmpty: Boolean =
    hbaseTable.rows.isEmpty ||
    hbaseTable.columns.isEmpty ||
    hbaseTable.versions.isEmpty

  def scan(): Iterator[KijiKeyValue] = {

    val itr: Iterator[Cell] = hbaseTable.scan()

    val entityIdCache: mutable.Map[Array[Byte], EntityId] = mutable.Map.empty
    val columnCache: Table[String, Array[Byte], KijiColumnName] = HashBasedTable.create()

    def getEntityId(bytes: Array[Byte]) = entityIdCache.get(bytes) match {
      case Some(eid) => eid
      case None => {
        val eid = entityIdFactory.getEntityIdFromHBaseRowKey(bytes)
        entityIdCache += bytes -> eid
        eid
      }
    }

    def getColumnName(family: String, qualifier: Array[Byte]) = Option(columnCache.get(family, qualifier)) match {
      case Some(columnName) => columnName
      case None => {
        val name = columnNameTranslator.toKijiColumnName(new HBaseColumnName(family.getBytes("utf-8"), qualifier))
        columnCache.put(family, qualifier, name)
        name
      }
    }

    def convertCell(cell: Cell): KijiKeyValue = {
      val eid: EntityId = getEntityId(cell.rowkey.bytes)
      val columnName: KijiColumnName = getColumnName(cell.family, cell.qualifier.bytes)


      val value: Any = cellDecoderProvider
        .getDecoder(columnName.getFamily, columnName.getQualifier)
        .decodeValue(cell.value)

      KijiKeyValue(eid, columnName, cell.version, value)
    }

    itr.map(convertCell)
  }

  case class KijiKeyValue(
    entityId: EntityId,
    column: KijiColumnName,
    version: Long,
    value: Any)
}
