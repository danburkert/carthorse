package carthorse

import java.{lang => jl, util => ju}

import scala.collection.JavaConverters.asScalaIteratorConverter

import org.hbase.async.{KeyValue, Scanner}
import carthorse.async.Deferred
import com.google.common.collect.Iterables
import scala.annotation.tailrec

trait ScanIterator[R, Q, V] extends Iterator[Cell[R, Q, V]] {
  /**
   * Close the HBase scan. This is automatically called when the iterator is exhausted, but it
   * should be called by the client if no further results are needed from an unfinished
   * ScanIterator.
   */
  def close(): Deferred[_]
}

object ScanIterator {

  def apply[R, Q, V](): ScanIterator[R, Q, V] = new EmptyScanIterator[R, Q, V]

  def apply[R <% Ordered[R], Q <% Ordered[Q], V : Ordering](
      scanner: Scanner,
      decodeRowkey: Array[Byte] => R,
      decodeQualifier: Array[Byte] => Q,
      decodeValue: Array[Byte] => V
  ): ScanIterator[R, Q, V] = new NonEmptyScanIterator(scanner, decodeRowkey, decodeQualifier, decodeValue)

  private final class NonEmptyScanIterator[R <% Ordered[R], Q <% Ordered[Q], V : Ordering](
    scanner: Scanner,
    decodeRowkey: Array[Byte] => R,
    decodeQualifier: Array[Byte] => Q,
    decodeValue: Array[Byte] => V)
    extends ScanIterator[R, Q, V] {
    private var nextBatch: Deferred[ju.ArrayList[ju.ArrayList[KeyValue]]] = scanner.nextRows()
    private var currentBatch: Iterator[KeyValue] = Iterator()
    private var finished: Boolean = false
    private val createCell: KeyValue => Cell[R, Q, V] = Cell(decodeRowkey, decodeQualifier, decodeValue)

    /** Load the next batch into `current`, and request a new batch for `next`. */
    private def requestBatch(): Unit = {
      val batch = nextBatch.result()
      if (batch == null) finished = true
      else {
        currentBatch = (Iterables.concat(batch): jl.Iterable[KeyValue]).iterator().asScala
        nextBatch = scanner.nextRows()
      }
    }

    @tailrec
    override def hasNext: Boolean = {
      if (finished) false
      else if (currentBatch.hasNext) true
      else {
        requestBatch()
        hasNext
      }
    }

    override def next(): Cell[R, Q, V] = createCell(currentBatch.next())

    override def close(): Deferred[_] = scanner.close()
  }

  private final class EmptyScanIterator[R, Q, V] extends ScanIterator[R, Q, V] {
    override def hasNext: Boolean = false
    override def next(): Cell[R, Q, V] = ???
    override def close(): Deferred[_] = Deferred.successful()
  }
}

