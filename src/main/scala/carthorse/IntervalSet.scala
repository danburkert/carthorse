package carthorse

import scala.Iterator
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.{mutable, SetLike}

import com.google.common.collect.{Range, RangeSet, TreeRangeSet, ImmutableRangeSet}

import carthorse.IntervalSet.IntervalSetBuilder

/**
 * Scala wrapper around the Guava [[com.google.common.collect.ImmutableRangeSet]].  Immutable.
 */
class IntervalSet[T <: Ordered[T]] private[carthorse] (val set: ImmutableRangeSet[T])
    extends Set[Interval[T]]
    with SetLike[Interval[T], IntervalSet[T]] {

  /** RangeSet Operations */

  /**
   * Returns the unique interval from this interval set that [[carthorse.Interval.contains]] the
   * point, or `None` if this interval set does not include the point.
   */
  def intervalContaining(point: T): Option[Interval[T]] =
    Option(set.rangeContaining(point)).map(new Interval(_))

  /**
   * Returns `true` if there exists an interval in this set which [[carthorse.Interval.encloses]]
   * the specified interval.
   */
  def encloses(interval: Interval[T]): Boolean = set.encloses(interval.range)

  /**
   * Returns `true` if for each interval in `other` there exists an interval in this set which
   * [[carthorse.Interval.encloses]] it.  It follows that all points in the other interval set are
   * contained in this interval set.
   */
  def encloses(other: IntervalSet[T]): Boolean = set.enclosesAll(other.set)

  /**
   * Returns the complement of this interval set.
   */
  def complement: IntervalSet[T] = new IntervalSet(set.complement)

  /** SortedSet[T]] Operations */

  /**
   * Returns `true` if any of the intervals in this set [[carthorse.Interval.contains]] the point.
   */
  def contains(point: T): Boolean = set.contains(point)

  /**
   * Returns a new interval set containing the specified point, as well as all points in this set.
   */
  def +(point: T): IntervalSet[T] = if (contains(point)) this else this + Interval(point)

  /**
   * Returns a new interval set containing all points in this set except the specified point.
   */
  def -(point: T): IntervalSet[T] = if (contains(point)) this - Interval(point) else this

  /**
   * Returns a new interval set containing all points in this set which fall in the specified range.
   */
  def range(range: Interval[T]): IntervalSet[T] = new IntervalSet(set.subRangeSet(range.range))

  /**
   * Returns a new interval set containing all points in this set which are less than the specified
   * point.
   */
  def lessThan(endpoint: T): IntervalSet[T] = range(Interval.lessThan(endpoint))

  /**
   * Returns a new interval set containing all points in this set which are less than or equal to
   * the specified point.
   */
  def atMost(endpoint: T): IntervalSet[T] = range(Interval.atMost(endpoint))

  /**
   * Returns a new interval set containing all points in this set which are greater than the
   * specified point.
   */
  def greaterThan(endpoint: T): IntervalSet[T] = range(Interval.greaterThan(endpoint))

  /**
   * Returns a new interval set containing all points in this set which are greater than or equal to
   * the specified point.
   */
  def atLeast(endpoint: T): IntervalSet[T] = range(Interval.atLeast(endpoint))

  /** Set[Interval[T]] Operations */

  protected[this] override def newBuilder: mutable.Builder[Interval[T], IntervalSet[T]] = new IntervalSetBuilder[T](empty)

  override def contains(elem: Interval[T]): Boolean = set.encloses(elem.range)

  override def +(interval: Interval[T]): IntervalSet[T] =
    if (encloses(interval)) this
    else new IntervalSetBuilder(this).+=(interval).result()

  override def -(elem: Interval[T]): IntervalSet[T] = new IntervalSetBuilder(this).-=(elem).result()

  override def empty: IntervalSet[T] = new IntervalSet(ImmutableRangeSet.of[T]())

  override def isEmpty: Boolean = set.isEmpty

  override def iterator: Iterator[Interval[T]] = set.asRanges().iterator().asScala.map(new Interval[T](_))

  override def stringPrefix: String =  "IntervalSet"

  override def equals(that: Any): Boolean = that match {
    case is: IntervalSet[T] => set.equals(is.set)
    case _ => false
  }
}

object IntervalSet {

  def empty[T <: Ordered[T]]: IntervalSet[T] = new IntervalSet(ImmutableRangeSet.of[T]())

  def all[T <: Ordered[T]]: IntervalSet[T] = new IntervalSet(ImmutableRangeSet.of(Range.all[T]))

  def apply[T <: Ordered[T]]: IntervalSet[T] = empty

  def apply[T <: Ordered[T]](elem: Interval[T]): IntervalSet[T] =
    new IntervalSet(ImmutableRangeSet.of(elem.range))

  def apply[T <: Ordered[T]](elems: Interval[T]*): IntervalSet[T] = apply(elems)

  def apply[T <: Ordered[T]](elems: TraversableOnce[Interval[T]]): IntervalSet[T] =
    (new IntervalSetBuilder[T](): IntervalSetBuilder[T]).++=(elems).result()

  def intersection[T <: Ordered[T]](elems: TraversableOnce[Interval[T]]): IntervalSet[T] =
    Intervals.intersection(elems).map(IntervalSet(_)).getOrElse(empty)

  def intersection[T <: Ordered[T]](elems: Interval[T]*): IntervalSet[T] = intersection(elems)

  private class IntervalSetBuilder[T <: Ordered[T]](initial: IntervalSet[T] = empty) extends mutable.Builder[Interval[T], IntervalSet[T]] {
    private val set = TreeRangeSet.create[T](initial.set)
    override def +=(elem: Interval[T]): this.type = { set.add(elem.range); this }
    override def clear(): Unit = set.clear()
    override def result(): IntervalSet[T] = new IntervalSet(ImmutableRangeSet.copyOf(set))
    def -=(elem: Interval[T]): this.type = { set.remove(elem.range); this }
    def addAll(other: RangeSet[T]): IntervalSetBuilder[T] = { set.addAll(other); this }
  }
}
