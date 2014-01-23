package carthorse

import java.{util => ju}
import java.{lang => jl}
import java.io.Closeable

import scala.collection.JavaConverters._
import scala.annotation.tailrec

import com.google.common.collect.Iterables
import continuum.bound.{Unbounded, Open, Closed}
import continuum.{Interval, IntervalSet}
import org.hbase.async.CompareFilter.CompareOp
import org.hbase.async.FilterList.Operator
import org.hbase.async._

import carthorse.async.Deferred
import carthorse.async.Deferred.su2ch

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
    rows: IntervalSet[RowKey] = IntervalSet(Interval.all[RowKey]),
    columns: Map[String, IntervalSet[Qualifier]],
    versions: IntervalSet[Long] = IntervalSet(Interval.atLeast(0)),
    maxVersions: Option[Int] = None,
    maxCells: Option[Int] = None,
    maxRows: Option[Int] = None,
    maxBytes: Option[Long] = None,
    populateBlockCache: Boolean = true
) {

  def viewRow(row: RowKey): HBaseTable = viewRows(row)

  def viewRows(rows: RowKey*): HBaseTable = viewRows(IntervalSet(rows.map(Interval(_)):_*))

  def viewRows(rows: Interval[RowKey]): HBaseTable = viewRows(IntervalSet(rows))

  def viewRows(rows: IntervalSet[RowKey]): HBaseTable = copy(rows = this.rows intersect rows)

  def viewFamily(family: String) = viewFamilies(family)

  def viewFamilies(family: String*) = copy(columns = columns filterKeys family.toSet)

  def viewColumn(family: String, qualifier: Qualifier): HBaseTable = viewColumns(family, qualifier)

  def viewColumns(family: String, qualifiers: Qualifier*): HBaseTable =
    viewColumns(family, IntervalSet(qualifiers.map(Interval(_)):_*))

  def viewColumns(family: String, qualifiers: IntervalSet[Qualifier]): HBaseTable =
    viewColumns(Map(family -> qualifiers))

  def viewColumns(columns: Map[String, IntervalSet[Qualifier]]): HBaseTable = {
    val mergedColumns = for ((family, qualifiers) <- columns)
      yield (family, this.columns.get(family).fold(qualifiers)((t: IntervalSet[Qualifier]) => t.intersect(qualifiers)))
    copy(columns = mergedColumns.filter{ case (family, qualifiers) => qualifiers.nonEmpty })
  }

  def viewVersion(version: Long): HBaseTable = viewVersions(version)

  def viewVersions(versions: Long*): HBaseTable = copy(versions =
      this.versions intersect IntervalSet(versions.map(Interval(_)):_*))

  def viewVersions(versions: Interval[Long]): HBaseTable =
    copy(versions = this.versions intersect versions)

  def maxVersions(maxVersions: Option[Int]): HBaseTable = copy(maxVersions = maxVersions)

  def maxCells(maxCells: Option[Int]): HBaseTable = copy(maxCells = maxCells)

  def maxRows(maxRows: Option[Int]): HBaseTable = copy(maxRows = maxRows)

  def maxBytes(maxBytes: Option[Long]): HBaseTable = copy(maxBytes = maxBytes)

  def populateBlockCache(populateBlockCache: Boolean): HBaseTable =
    copy(populateBlockCache = populateBlockCache)

  def isEmpty: Boolean = rows.isEmpty || columns.isEmpty || versions.isEmpty

  /**
   * Filter for filtering all cells not in a row contained in the interval set of rows. Not
   * necessary if the interval set consists of a single interval, or entirely of specified rows.
   */
  private lazy val rowsFilter: Option[ScanFilter] =
    if (rows.size <= 1) None
    else {
      val filters = rows.toSeq.map { interval =>
        val start: Option[ScanFilter] = interval.lower.bound match {
          case Closed(l)   => Some(new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(l.bytes)))
          case Open(l)     => Some(new RowFilter(CompareOp.GREATER, new BinaryComparator(l.bytes)))
          case Unbounded() => None
        }
        val end: Option[ScanFilter] = interval.upper.bound match {
          case Closed(u)   => Some(new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(u.bytes)))
          case Open(u)     => Some(new RowFilter(CompareOp.LESS, new BinaryComparator(u.bytes)))
          case Unbounded() => None
        }

        val bounds = List(start, end).flatten
        bounds.size match {
          case 2 => new FilterList(bounds.asJava, Operator.MUST_PASS_ALL)
          case 1 => bounds.head
          case _ => throw new AssertionError()
        }
      }
      Some(new FilterList(filters.asJava, Operator.MUST_PASS_ONE))
    }

  /**
   * Filter for filtering all cells not in the map of column family to qualifier interval set.
   * Column families which include all qualifiers, or only specified qualifiers (not intervals)
   * do not need a filter.
   */
  private lazy val columnsFilter: Option[ScanFilter] = {
    /** Returns a filter which filters any cell not in the specified family and qualifiers. */
    def familyFilter(family: String, qualifiers: IntervalSet[Qualifier]): Option[ScanFilter] = {
      if (qualifiers.forall(_.isPoint)) return None
      if (qualifiers.contains(Interval.atLeast(Identifier.minimum))) return None

      val qualifierFilters: Seq[ScanFilter] = qualifiers.toSeq.map { interval =>
        val (startKey, startInclusive) = interval.lower.bound match {
          case Closed(l) => l.bytes -> true
          case Open(l) => l.bytes -> false
          case Unbounded() => (null: Array[Byte]) -> true
        }
        val (endKey, endInclusive) = interval.upper.bound match {
          case Closed(u) => u.bytes -> true
          case Open(u) => u.bytes -> false
          case Unbounded() => (null: Array[Byte]) -> true
        }
        new ColumnRangeFilter(startKey, startInclusive, endKey, endInclusive)
      }

      val qualifiersFilter =
        if (qualifierFilters.size == 1) qualifierFilters.head
        else new FilterList(qualifierFilters.asJava, Operator.MUST_PASS_ONE)

      val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family.getBytes(Charset)))
      Some(new FilterList(List(familyFilter, qualifiersFilter).asJava, Operator.MUST_PASS_ALL))
    }

    val familyFilters: Seq[ScanFilter] = columns.toSeq.flatMap { case (family, qualifiers) =>
      familyFilter(family, qualifiers)
    }

    familyFilters.size match {
      case 0 => None
      case 1 => Some(familyFilters.head)
      case _ => Some(new FilterList(familyFilters.asJava, Operator.MUST_PASS_ONE))
    }
  }

  /**
   * Filter for filtering all cells not in the list of versions.
   */
  private lazy val versionsFilter: Option[TimestampsFilter] =
    if (versions.size <= 1) None
    else {
      require(versions.forall(_.isPoint))
      val timestamps: Seq[java.lang.Long] = versions.toSeq.map { interval =>
        interval.lower.bound match {
          case Closed(l) => l: java.lang.Long
          case _         => throw new AssertionError()
        }
      }
      Some(new TimestampsFilter(timestamps:_*))
    }


  /**
   * Return a scanner over the table view, or None if it can be determined that the Scanner would
   * contain no results.
   */
  private def getScanner(): Option[Scanner] = {
    val scanner = client.newScanner(name)

    // set start and stop rowkey
    // short circuit if no rows
    if (rows.isEmpty) return None
    val (startKey, stopKey): (Option[RowKey], Option[RowKey]) = rows.span.map(_.normalize).getOrElse(None -> None)
    startKey.foreach(s => scanner.setStartKey(s.bytes))
    // asynchbase uses the empty byte array as a special stop key to signify 'scan to end of table',
    // so if this table's stop key is the empty array, we replace it with Array(0x00).
    stopKey.foreach(s => scanner.setStopKey(if (s.bytes.isEmpty) Array[Byte](0) else s.bytes))

    // set columns
    val (families, qualifiers): (Seq[Array[Byte]], Seq[Array[Array[Byte]]]) = columns.map {
      case (family: String, qualifiers: IntervalSet[Qualifier]) => {
        // If all qualifiers in the family are specified as points we can scan just the
        // requested qualifiers.  Otherwise, we rely on filters to exclude unwanted qualifiers.
        val fam: Array[Byte] = family.getBytes(Charset)
        // asynchbase uses null as an indicator to scan all qualifiers in the column family
        val quals: Array[Array[Byte]] =
          if (qualifiers.forall(_.isPoint)) qualifiers.map(_.point.get.bytes).toArray
          else null
        fam -> quals
      }
    }.toSeq.unzip
    // sanity checking
    require(families.size == qualifiers.length)
    // short circuit if no families
    if (families.isEmpty) return None
    scanner.setFamilies(families.toArray, qualifiers.toArray)

    // set versions
    val (minVersion, maxVersion) = versions.span.map(_.normalize).getOrElse(None -> None)
    // short circuit if no versions
    if (minVersion.isEmpty && maxVersion.isEmpty) return None
    minVersion.foreach(v => scanner.setMinTimestamp(v))
    maxVersion.foreach(v => scanner.setMaxTimestamp(v))

    // apply filters
    val filters = List(rowsFilter, columnsFilter, versionsFilter).flatten
    filters.size match {
      case 0 =>
      case 1 => scanner.setFilter(filters.head)
      case _ => scanner.setFilter(new FilterList(filters.asJava, Operator.MUST_PASS_ALL))
    }

    Some(scanner)
  }

  class ScanIterator(scanner: Scanner) extends Iterator[Cell] with Closeable {
    private var nextBatch: Deferred[ju.ArrayList[ju.ArrayList[KeyValue]]] = scanner.nextRows()
    private var currentBatch: Iterator[KeyValue] = Iterator()
    private var finished: Boolean = false

    /** Load the next batch into `current`, and request a new batch for `next`. */
    private def requestBatch(): Unit = {
      val batch = nextBatch.join()
      if (batch == null) finished = true
      else {
        currentBatch = (Iterables.concat(batch): jl.Iterable[KeyValue]).iterator().asScala
        nextBatch = scanner.nextRows()
      }
    }

    @tailrec
    final def hasNext: Boolean = {
      if (finished) false
      else if (currentBatch.hasNext) true
      else {
        requestBatch()
        hasNext
      }
    }

    def close(): Unit = scanner.close().join()
    def next(): Cell = Cell(currentBatch.next())
  }

  object EmptyScanIterator extends Iterator[Cell] with Closeable {
    def hasNext: Boolean = false
    def next(): Cell = null
    def close(): Unit = {}
  }

  def scan(): Iterator[Cell] with Closeable =
    getScanner().fold(EmptyScanIterator: Iterator[Cell] with Closeable)(new ScanIterator(_))
}

object HBaseTable {
  def apply(client: HBaseClient, name: String, families: Iterable[String]): HBaseTable =
    HBaseTable(client, name, columns = families.map(_ -> IntervalSet(Interval.all[Qualifier])).toMap)
}
