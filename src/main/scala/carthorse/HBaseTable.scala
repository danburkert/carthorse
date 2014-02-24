package carthorse

import java.{util => ju}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

import continuum.bound.{Unbounded, Open, Closed}
import continuum.{Interval, IntervalSet}
import org.hbase.async.CompareFilter.CompareOp
import org.hbase.async.FilterList.Operator
import org.hbase.async._
import carthorse.async.Deferred
import carthorse.async.Deferred._

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
case class HBaseTable[R <% Ordered[R], Q <% Ordered[Q], V : Ordering](
    client: HBaseClient,
    name: String,
    rows: IntervalSet[Array[Byte]] = IntervalSet(Interval.all[Array[Byte]]),
    columns: Map[String, IntervalSet[Array[Byte]]],
    versions: IntervalSet[Long] = IntervalSet(Interval.atLeast(0)),
    maxVersions: Option[Int] = None,
    maxCells: Option[Int] = None,
    maxRows: Option[Int] = None,
    maxBytes: Option[Long] = None,
    populateBlockCache: Boolean = true,
    encodeRowkey: R => Array[Byte],
    decodeRowkey: Array[Byte] => R,
    encodeQualifier: Q => Array[Byte],
    decodeQualifier: Array[Byte] => Q,
    encodeValue: V => Array[Byte],
    decodeValue: Array[Byte] => V
) {

  type Table = HBaseTable[R, Q, V]

  def viewRow(row: R): Table = viewRows(row)

  def viewRows(rows: R*): Table = viewRows(IntervalSet(rows.map(Interval(_)):_*))

  def viewRows(rows: Interval[R]): Table = viewRows(IntervalSet(rows))

  def viewRows(rows: IntervalSet[R]): Table =
    viewRowsRaw(rows.map(i => i.map(encodeRowkey)))

  def viewRowsRaw(rows: IntervalSet[Array[Byte]]): Table =
    copy(rows = this.rows.intersect(rows))

  def viewFamily(family: String): Table = viewFamilies(family)

  def viewFamilies(family: String*): Table = copy(columns = columns filterKeys family.toSet)

  def viewColumn(family: String, qualifier: Q): Table =
    viewColumns(family, qualifier)

  def viewColumns(family: String, qualifiers: Q*): Table =
    viewColumns(family, IntervalSet(qualifiers.map(Interval(_)):_*))

  def viewColumns(family: String, qualifiers: IntervalSet[Q]): Table =
    viewColumns(Map(family -> qualifiers))

  def viewColumns(columns: Map[String, IntervalSet[Q]]): Table = {
    val rawColumns = columns.mapValues(qs => qs.map(q => q.map(encodeQualifier)))
    viewColumnsRaw(rawColumns)
  }

  def viewColumnsRaw(rawColumns: Map[String, IntervalSet[Array[Byte]]]): Table = {
    val mergedColumns: Map[String, IntervalSet[Array[Byte]]] = for {
      (family, additional) <- rawColumns
      existing <- this.columns.get(family)
      intersection = existing.intersect(additional)
      if intersection.nonEmpty
    } yield (family, intersection)
    copy(columns = mergedColumns)
  }

  def viewVersion(version: Long): Table = viewVersions(version)

  def viewVersions(versions: Long*): Table = copy(versions =
      this.versions intersect IntervalSet(versions.map(Interval(_)):_*))

  def viewVersions(versions: Interval[Long]): Table =
    copy(versions = this.versions intersect versions)

  def maxVersions(maxVersions: Option[Int]): Table = copy(maxVersions = maxVersions)

  def maxCells(maxCells: Option[Int]): Table = copy(maxCells = maxCells)

  def maxRows(maxRows: Option[Int]): Table = copy(maxRows = maxRows)

  def maxBytes(maxBytes: Option[Long]): Table = copy(maxBytes = maxBytes)

  def populateBlockCache(populateBlockCache: Boolean): Table =
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
          case Closed(l)   => Some(new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(l)))
          case Open(l)     => Some(new RowFilter(CompareOp.GREATER, new BinaryComparator(l)))
          case Unbounded() => None
        }
        val end: Option[ScanFilter] = interval.upper.bound match {
          case Closed(u)   => Some(new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(u)))
          case Open(u)     => Some(new RowFilter(CompareOp.LESS, new BinaryComparator(u)))
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
    def familyFilter(family: String, qualifiers: IntervalSet[Array[Byte]]): Option[ScanFilter] = {
      if (qualifiers.forall(_.isPoint)) return None
      if (qualifiers.contains(Interval.atLeast(Array[Byte]()))) return None

      val qualifierFilters: Seq[ScanFilter] = qualifiers.toSeq.map { interval =>
        val (startKey, startInclusive) = interval.lower.bound match {
          case Closed(l) => l -> true
          case Open(l) => l -> false
          case Unbounded() => (null: Array[Byte]) -> true
        }
        val (endKey, endInclusive) = interval.upper.bound match {
          case Closed(u) => u -> true
          case Open(u) => u -> false
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
  private def createScanner(): Option[Scanner] = {
    val scanner = client.newScanner(name)

    // set start and stop rowkey
    // short circuit if no rows
    if (rows.isEmpty) return None
    val (startKey, stopKey): (Option[Array[Byte]], Option[Array[Byte]]) = rows.span.map(_.normalize).getOrElse(None -> None)
    startKey.foreach(s => scanner.setStartKey(s))
    // asynchbase uses the empty byte array as a special stop key to signify 'scan to end of table',
    // so if this table's stop key is the empty array, we replace it with Array(0x00).
    stopKey.foreach(s => scanner.setStopKey(if (s.isEmpty) Array[Byte](0) else s))

    // set columns
    val (families, qualifiers): (Seq[Array[Byte]], Seq[Array[Array[Byte]]]) = columns.map {
      case (family: String, qualifiers: IntervalSet[Array[Byte]]) => {
        // If all qualifiers in the family are specified as points we can scan just the
        // requested qualifiers.  Otherwise, we rely on filters to exclude unwanted qualifiers.
        val familyBytes: Array[Byte] = family.getBytes(Charset)
        // asynchbase uses null as an indicator to scan all qualifiers in the column family
        val qualifiersBytes: Array[Array[Byte]] =
          if (qualifiers.forall(_.isPoint)) qualifiers.map(_.point.get).toArray
          else null
        familyBytes -> qualifiersBytes
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


  def scan(): ScanIterator[R, Q, V] =
    createScanner().map(ScanIterator(_, decodeRowkey, decodeQualifier, decodeValue)).getOrElse(ScanIterator())

  def put(cells: Cell[R, Q, V]*): Unit = put(cells)

  def put(cells: Seq[Cell[R, Q, V]]): Deferred[_] = {
    val keyvalues: Array[KeyValue] = {
      val kvs: ArrayBuffer[KeyValue] = cells match {
        case _: IndexedSeq[_] => new ArrayBuffer[KeyValue](cells.size) // IndexedSeq guarantees constant time .size call
        case _ => new ArrayBuffer[KeyValue]()
      }
      val createKeyValue: Cell[R, Q, V] => KeyValue = Cell(encodeRowkey, encodeQualifier, encodeValue)
      cells.foreach(cell => kvs += createKeyValue(cell))
      kvs.toArray
    }

    // sorted by rowkey, then family, then qualifier, then timestamp, then value
    ju.Arrays.sort(keyvalues.asInstanceOf[Array[AnyRef]]) // TODO: figure out why the cast is necessary

    var startRI = 0 // start row index. Keeps track of the first index of the current rowkey batch

    val puts = ListBuffer[Deferred[AnyRef]]()

    while (startRI < keyvalues.length) {
      var stopRI = startRI + 1 // stop row index. Keeps track of the first index of the next rowkey batch

      val rowkey = keyvalues(startRI).key() // keep track of current rowkey
      var family = keyvalues(startRI).family() // keep track of current family
      val familyIs = ArrayBuffer[Int](startRI) // family indices. Keeps track of the start indices for each family in the current row

      // set stopRI for current row and keep track of the subsequent family start indices
      while(stopRI < keyvalues.length && ju.Arrays.equals(rowkey, keyvalues(stopRI).key())) {
        if (! ju.Arrays.equals(family, keyvalues(stopRI).family())) {
          familyIs += stopRI
          family = keyvalues(stopRI).family()
        }
        stopRI += 1
      }

      val numFamilies = familyIs.size

      val families: Array[Array[Byte]] = Array.ofDim(numFamilies)
      val qualifiers: Array[Array[Array[Byte]]] = Array.ofDim(numFamilies)
      val timestamps: Array[Array[Long]] = Array.ofDim(numFamilies)
      val values: Array[Array[Array[Byte]]] = Array.ofDim(numFamilies)

      familyIs += stopRI // stop index for last family
      for (familyI <- 0 until numFamilies) {
        val startFI = familyIs(familyI)
        val stopFI = familyIs(familyI + 1)
        val numQualifiers = stopFI - startFI

        families(familyI) = keyvalues(startFI).family()
        qualifiers(familyI) = Array.ofDim[Array[Byte]](numQualifiers)
        timestamps(familyI) = Array.ofDim[Long](numQualifiers)
        values(familyI) = Array.ofDim[Array[Byte]](numQualifiers)

        for (qualifierI <- 0 until numQualifiers) {
          val keyvalue = keyvalues(startFI + qualifierI)
          qualifiers(familyI)(qualifierI) = keyvalue.qualifier()
          timestamps(familyI)(qualifierI) = keyvalue.timestamp()
          values(familyI)(qualifierI) = keyvalue.value()
        }
      }

      puts += client put new PutRequest(name.getBytes(Charset), rowkey, families, qualifiers, values, timestamps)

      // reset for next iteration
      startRI = stopRI
      familyIs.clear()
    }
    Deferred.sequence(puts.toList)
  }

  // TODO: rip out this abomination
  def deleteRows(rows: R*): Deferred[_] = {
    val n = name.getBytes(Charset)
    val deletes = rows.map(row => su2ch(client.delete(new DeleteRequest(n, encodeRowkey(row)))))
    Deferred.sequence(deletes)
  }

  def flush(): Deferred[_] = client.flush()
}

object HBaseTable {

  def rawTable(client: HBaseClient, name: String, families: Iterable[String]): HBaseTable[Array[Byte], Array[Byte], Array[Byte]] =
    HBaseTable(
      client,
      name,
      columns = families.map(_ -> IntervalSet(Interval.all[Array[Byte]])).toMap,
      encodeRowkey = identity[Array[Byte]],
      decodeRowkey = identity[Array[Byte]],
      encodeQualifier = identity[Array[Byte]],
      decodeQualifier = identity[Array[Byte]],
      encodeValue = identity[Array[Byte]],
      decodeValue = identity[Array[Byte]])

  def apply[R <% Ordered[R] : OrderedByteable, Q <% Ordered[Q] : OrderedByteable, V <% Ordered[V] : Byteable]
  (client: HBaseClient, name: String, families: Iterable[String]): HBaseTable[R, Q, V] =
    HBaseTable(
      client,
      name,
      columns = families.map(_ -> IntervalSet(Interval.all[Array[Byte]])).toMap,
      encodeRowkey = OrderedByteable.toBytesAsc[R],
      decodeRowkey = OrderedByteable.fromBytesAsc[R],
      encodeQualifier = OrderedByteable.toBytesAsc[Q],
      decodeQualifier = OrderedByteable.fromBytesAsc[Q],
      encodeValue = Byteable.toBytes[V],
      decodeValue = Byteable.fromBytes[V])
}
