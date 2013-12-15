package carthorse

import java.util.ArrayList

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import org.hbase.async.{HBaseClient, KeyValue}
import continuum.{Interval, IntervalSet}

import carthorse.async.Deferred

case class HBaseTable(
    name: String,
    client: HBaseClient,
    rows: IntervalSet[RowKey] = IntervalSet(Interval.all[RowKey]),
    columns: Map[String, IntervalSet[Qualifier]] = Map().withDefaultValue(IntervalSet(Interval.all[Qualifier])),
    versions: IntervalSet[Long] = IntervalSet(Interval.all[Long])) {

  def row(row: RowKey): HBaseTable = rows(row)

  def rows(rows: RowKey*): HBaseTable = this.rows(IntervalSet(rows.map(Interval(_)):_*))

  def rows(rows: Interval[RowKey]): HBaseTable = this.rows(IntervalSet(rows))

  def rows(rows: IntervalSet[RowKey]): HBaseTable = copy(rows = this.rows intersect rows)

  def family(family: String) = families(family)

  def families(family: String*) = copy(columns = columns filterKeys family.toSet)

  def column(family: String, qualifier: Qualifier): HBaseTable = columns(family, qualifier)

  def columns(family: String, qualifiers: Qualifier*): HBaseTable =
    columns(family, IntervalSet(qualifiers.map(Interval(_)):_*))

  def columns(family: String, qualifiers: Interval[Qualifier]): HBaseTable =
    columns(family, IntervalSet(qualifiers))

  def columns(family: String, qualifiers: IntervalSet[Qualifier]): HBaseTable =
    copy(columns = Map(family -> (this.columns(family) intersect qualifiers)))

  def columns(columns: Map[String, IntervalSet[Qualifier]]): HBaseTable = {
    val mergedColumns = for ((family, qualifiers) <- columns)
                        yield (family, columns(family) intersect qualifiers)
    copy(columns = mergedColumns)
  }

  def version(version: Long): HBaseTable = versions(version)

  def versions(versions: Long*): HBaseTable = copy(versions =
      this.versions intersect IntervalSet(versions.map(Interval(_)):_*))

  def versions(versions: Interval[Long]): HBaseTable = this.versions(IntervalSet(versions))

  def versions(versions: IntervalSet[Long]): HBaseTable = copy(versions = this.versions intersect versions)


  def foreach[U](f: Cell => U): Unit = {
    val scanner = client.newScanner(name)
    if (columns.nonEmpty) scanner.setFamilies(columns.keys.map(_.getBytes(Charset)).toArray, null)
    val (start, stop) = rows.span.normalize
    start.foreach(s => scanner.setStartKey(s.bytes))
    stop.foreach(s => scanner.setStopKey(s.bytes))

    new Deferred(scanner.nextRows()).foreach { kvGroups: ArrayList[ArrayList[KeyValue]] =>
      for {
        kvGroup <- kvGroups.asScala
        kv <- kvGroup.asScala
      } f(Cell(kv))
    }
  }
}

object HBaseTable {
  def apply(name: String, client: HBaseClient, families: Iterable[String]): HBaseTable =
    HBaseTable(name, client, columns = families.map(_ -> IntervalSet(Interval.all[Qualifier])).toMap)
}
