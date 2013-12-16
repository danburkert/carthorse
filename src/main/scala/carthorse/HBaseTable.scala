package carthorse

import java.{util => ju}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import org.hbase.async._
import continuum.{Interval, IntervalSet}

import carthorse.async.Deferred.su2ch
import carthorse.async.Deferred
import scala.concurrent.duration.Duration

/**
 * A view of an HBase table. Clients can use an instance of this class to perform scans, gets,
 * and puts against an HBase table. The view of the table may be restricted by rows, columns, and/or
 * versions. The table will not be able to read cells from the table which fall outside this view.
 * Further restricted views of the table can be created with the appropriate methods which return
 * an HBaseTable.
 *
 * HBaseTable instances are immutable and thread safe. All operations are by default non-blocking,
 * and return values wrapped in a [[carthorse.async.Deferred]]. Where appropriate, blocking variants
 * of these operations are provided with a _! suffix. Where possible, clients should prefer the
 * non-blocking variants.
 *
 * @param client [[org.hbase.async.HBaseClient]] used to execute operations.
 * @param name of the table.
 * @param columns IntervalSet of columns viewable by this table.
 * @param rows IntervalSet of rowkeys viewable by this table.
 * @param versions IntervalSet of versions viewable by this table.
 *
 * @param maxVersions maximum number of versions to return from get and scan requests. Returned
 *        results will be the most recent, up to the maximum number specified.
 * @param maxCells a performance tuning parameter which limits the maximum number of cells fetched
 *        by the client in a single RPC request.
 * @param maxRows a performance tuning parameter which limits the maximum number of rows fetched by
 *        the client in a single RPC request.
 * @param maxBytes a performance tuning parameter which limits the maximum number of bytes fetched
 *        by the client in a single RPC request.  HBase 0.96+, ignored for earlier versions.
 */
case class HBaseTable(
    client: HBaseClient,
    name: String,
    columns: Map[String, IntervalSet[Qualifier]],
    rows: IntervalSet[RowKey] = IntervalSet(Interval.all[RowKey]),
    versions: IntervalSet[Long] = IntervalSet(Interval.all[Long]),
    maxVersions: Int = Int.MaxValue,
    maxCells: Int = Scanner.DEFAULT_MAX_NUM_KVS,
    maxRows: Int = Scanner.DEFAULT_MAX_NUM_ROWS,
    maxBytes: Long = 0xFFFFFFFFF0000000L,  // => max = 256MB
    populateBlockCache: Boolean = true
) {

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

  def foreach[U](f: Cell => U): Deferred[Unit] =
    scanner().nextRows().map { kvGroups: ju.ArrayList[ju.ArrayList[KeyValue]] =>
      for {
        kvGroup <- kvGroups.asScala
        kv <- kvGroup.asScala
      } f(Cell(kv))
    }

  def foreach_![U](f: Cell => U): Unit = foreach(f).result()
  def foreach_![U](f: Cell => U, atMost: Duration): Unit = foreach(f).result()

  def iterator: Deferred[Iterator[Cell]] = {
    val scan = scanner()

    ???
  }

  private lazy val qualifiersFilter: ScanFilter = {
    columns.map { case (family, qualifiers) =>
      qualifiers

    }

    ???
  }

  private lazy val rowsFilter: ScanFilter = {
    ???
  }

  def scan(
      maxVersions: Int = maxVersions,
      maxCells: Int = maxCells,
      maxRows: Int = maxRows,
      maxBytes: Long = maxBytes,
      populateBlockCache: Boolean = populateBlockCache
  ): Deferred[Iterator[Cell]] = {
    val scanner = client.newScanner(name)
    var filters: List[ScanFilter] = List()
    if (columns.nonEmpty) scanner.setFamilies(columns.keys.map(_.getBytes(Charset)).toArray, null)

    val (startKey, stopKey) = rows.span.normalize
    startKey.foreach(s => scanner.setStartKey(s.bytes))
    stopKey.foreach(s => scanner.setStopKey(s.bytes))

    val (minVersion, maxVersion) = versions.span.normalize
    minVersion.foreach(v => scanner.setMinTimestamp(v))
    maxVersion.foreach(v => scanner.setMaxTimestamp(v))
    scanner

    ???
  }

  /**
   * Creates a [[org.hbase.async.Scanner]] over this table. Scanner instance are *not* thread safe.
   * Scanners will automatically close themselves when they are out of rows, or if they timeout.
   * Scanners may be closed explicitly if not used fully.
   */
  private def scanner(): Scanner = {
    val scanner = client.newScanner(name)
    if (columns.nonEmpty) scanner.setFamilies(columns.keys.map(_.getBytes(Charset)).toArray, null)
    val (start, stop) = rows.span.normalize
    start.foreach(s => scanner.setStartKey(s.bytes))
    stop.foreach(s => scanner.setStopKey(s.bytes))
    scanner
  }
}

object HBaseTable {
  def apply(client: HBaseClient, name: String, families: Iterable[String]): HBaseTable =
    HBaseTable(client, name, families.map(_ -> IntervalSet(Interval.all[Qualifier])).toMap)
}
