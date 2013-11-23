package carthorse

import scala.collection.JavaConverters.asJavaIterableConverter

import com.google.common.collect.{BoundType, Range}

/**
 * Scala wrapper around the Guava [[com.google.common.collect.Range]].  Immutable.
 */
class Interval[T <: Ordered[T]] private[carthorse] (val range: Range[T]) extends ((T) => Boolean) {

  /**
   * Returns `true` if this interval has a lower endpoint.
   */
  def hasLowerBound: Boolean = range.hasLowerBound

  /**
   * Returns the lower endpoint of this interval.
   *
   * @throws IllegalStateException if this interval is unbounded below (that is,
   *    [[carthorse.Interval.hasLowerBound]] returns `false`.
   */
  def lowerEndpoint = range.lowerEndpoint

  /**
   * Returns the type of this interval's lower bound: [[com.google.common.collect.BoundType.CLOSED]]
   * if the interval includes its lower endpoint, [[com.google.common.collect.BoundType.OPEN]] if it
   * does not.
   *
   * @throws IllegalStateException if this interval is unbounded below (that is,
   *    [[carthorse.Interval.hasLowerBound]] returns `false`)
   */
  def lowerBoundType: BoundType = range.lowerBoundType

  /**
   * Returns `true` if this interval has an upper endpoint.
   */
  def hasUpperBound: Boolean = range.hasUpperBound

  /**
   * Returns the upper endpoint of this interval.
   *
   * @throws IllegalStateException if this interval is unbounded below (that is,
   *    [[carthorse.Interval.hasUpperBound]] returns `false`.
   */
  def upperEndpoint = range.upperEndpoint

  /**
   * Returns the type of this interval's upper bound: [[com.google.common.collect.BoundType.CLOSED]]
   * if the interval includes its upper endpoint, [[com.google.common.collect.BoundType.OPEN]] if it
   * does not.
   *
   * @throws IllegalStateException if this interval is unbounded below (that is,
   *    [[carthorse.Interval.hasUpperBound]] returns `false`)
   */
  def upperBoundType: BoundType = range.upperBoundType

  /**
   * Returns `true` if this interval is of the form `[v..v)` or `(v..v]`. (This does not encompass
   * intervals of the form `(v..v)`, because such intervals are <i>invalid</i> and
   * can't be constructed at all.)
   *
   * Note that certain discrete intervals such as the integer interval `(3..4)` are <b>not</b>
   * considered empty, even though they contain no actual values.  In these cases, it may be
   * helpful to preprocess intervals with [[com.google.common.collect.DiscreteDomain]].
   */
  def isEmpty: Boolean = range.isEmpty

  /**
   * Returns `true` if `value` is within the bounds of this interval. For example, on the
   * interval `[0..2)`, `contains(1)` returns `true`, while `contains(2)` returns `false`.
   */
  def contains(value: T): Boolean = range contains value

  /**
   * Equivalent to [[carthorse.Interval.contains]].  Allows `Interval`s to be used as a predicate.
   */
  def apply(value: T): Boolean = range contains value

  /**
   * Returns `true` if every element in `values` is contained in this interval.
   */
  def contains(values: Iterable[T]): Boolean = range containsAll values.asJava

  /**
   * Returns `true` if the bounds of `other` do not extend outside the bounds of this interval.
   * Examples:
   *
   * <ul>
   * <li>`[3..6]` encloses `[4..5]`
   * <li>`(3..6)` encloses `(3..6)`
   * <li>`[3..6]` encloses `[4..4)` (even though the latter is empty)
   * <li>`(3..6]` does not enclose `[3..6]`
   * <li>`[4..5]` does not enclose `(3..6)` (even though it contains every value
   *     contained by the latter interval)
   * <li>`[3..6]` does not enclose `(1..1]` (even though it contains every value
   *     contained by the latter interval)
   * </ul>
   *
   * <p>Note that if `a.encloses(b)`, then `b.contains(v)` implies `a.contains(v)`, but as the last
   * two examples illustrate, the converse is not always true.
   *
   * <p>Being reflexive, antisymmetric and transitive, the `encloses` relation defines a
   * <i>partial order</i> over intervals. There exists a unique maximal interval according to this
   * relation, and also numerous minimal intervals. Enclosure also implies connectedness.
   */
  def encloses(other: Interval[T]): Boolean = range encloses other.range

  /**
   * Returns `true` if there exists a (possibly empty) interval which is enclosed by both this
   * interval and `other`.
   *
   * <p>For example,
   * <ul>
   * <li>`[2, 4)` and `[5, 7)` are not connected
   * <li>`[2, 4)` and `[3, 5)` are connected, because both enclose `[3, 4)`
   * <li>`[2, 4)` and `[4, 6)` are connected, because both enclose the empty interval `[4, 4)`
   * </ul>
   *
   * <p>Note that this interval and `other` have a well-defined [[carthorse.Interval.union]] and
   * [[carthorse.Interval.intersection]] (as a single, possibly-empty interval) if and only if this
   * method returns `true`.
   *
   * <p>The connectedness relation is both reflexive and symmetric, but does not form an
   * equivalence relation as it is not transitive.
   *
   * <p>Note that certain discrete intervals are not considered connected, even though there are no
   * elements "between them."  For example, `[3, 5]` is not considered connected to
   * `[6, 10]`.  In these cases, it may be desirable for both input intervals to be preprocessed
   * with [[com.google.common.collect.DiscreteDomain]] before testing for connectedness.
   */
  def connected(other: Interval[T]): Boolean = range isConnected other.range

  /**
   * Returns the maximal interval enclosed by both this interval and `other`, or `None` if no such
   * interval exists.
   *
   * <p>For example, the intersection of `[1..5]` and `(3..7)` is `(3..5]`. The resulting interval
   * may be empty; for example, `[1..5)` intersected with `[5..7)` yields the empty interval
   * `[5..5)`.
   *
   * <p>The intersection exists if and only if the two intervals are
   * [[carthorse.Interval.connected]].
   *
   * <p>The intersection operation is commutative, associative and idempotent, and its identity
   * element is [[carthorse.Interval.all]]).
   */
  def intersection(other: Interval[T]): Option[Interval[T]] =
    if (connected(other)) Some(new Interval(range intersection other.range)) else None

  /**
   * Returns the maximal interval that encloses points in this and `other`, or both.  Returns `None`
   * if no such interval exists.
   *
   * <p>For example, the union of `[1..5]` and `(3..7)` is `(1..7]`. There may be no resulting
   * interval; for example, `[1..5)` union `[5..7)` yields the empty interval `[5..5)`.
   *
   * <p>The intersection exists if and only if the two intervals are
   * [[carthorse.Interval.connected]].
   *
   * <p>The intersection operation is commutative, associative and idempotent, and its identity
   * element is [[carthorse.Interval.all]]).
   */
  def union(other: Interval[T]): Option[Interval[T]] =
    if (connected(other)) Some(span(other)) else None

  /**
   * Returns the minimal interval that [[carthorse.Interval.encloses]] both this interval and
   * `other`. For example, the span of `[1..3]` and `(5..7)` is `[1..7)`.
   *
   * <p><i>If</i> the input intervals are [[carthorse.Interval.connected]], the returned interval
   * can also be called their <i>union</i>. If they are not, note that the span might contain values
   * that are not contained in either input interval.
   *
   * <p>Like [[carthorse.Interval.intersection]], this operation is commutative, associative
   * and idempotent. Unlike it, it is always well-defined for any two input intervals.
   */
  def span(other: Interval[T]): Interval[T] = new Interval(range.span(other.range))
}

/**
 * Provides factory functions for intervals.
 */
object Interval {

  def open[T <: Ordered[T]](lower: T, upper: T): Interval[T] =
    new Interval(Range.open[T](lower, upper))

  def closed[T <: Ordered[T]](lower: T, upper: T): Interval[T] =
    new Interval(Range.closed(lower, upper))

  def closedOpen[T <: Ordered[T]](lower: T, upper: T): Interval[T] =
    new Interval(Range.closedOpen(lower, upper))

  def apply[T <: Ordered[T]](lower: T, upper: T): Interval[T]
    = closedOpen(lower, upper)

  def openClosed[T <: Ordered[T]](lower: T, upper: T): Interval[T] =
    new Interval(Range.openClosed(lower, upper))

  def lessThan[T <: Ordered[T]](endpoint: T): Interval[T] =
    new Interval(Range lessThan endpoint)

  def atMost[T <: Ordered[T]](endpoint: T): Interval[T] =
    new Interval(Range atMost endpoint)

  def greaterThan[T <: Ordered[T]](endpoint: T): Interval[T] =
    new Interval(Range greaterThan endpoint)

  def atLeast[T <: Ordered[T]](endpoint: T): Interval[T] =
    new Interval(Range atLeast endpoint)

  def all[T <: Ordered[T]]: Interval[T] =
    new Interval(Range.all[T])

  def apply[T <: Ordered[T]](point: T): Interval[T] =
    new Interval(Range.singleton(point))
}

/**
 * Provides utility functions for operating on collections of intervals.
 */
object Intervals {

  def span[T <: Ordered[T]](values: TraversableOnce[Interval[T]]): Interval[T] =
    values.reduce((a: Interval[T], b: Interval[T]) => a.span(b))

  def intersection[T <: Ordered[T]](values: TraversableOnce[Interval[T]]): Option[Interval[T]] = {
    val z = values.toStream.headOption
    values.foldLeft(z)((acc: Option[Interval[T]], i: Interval[T]) => acc.flatMap(_ intersection i))
  }

  def union[T <: Ordered[T]](values: TraversableOnce[Interval[T]]): Option[Interval[T]] = {
    val z = values.toStream.headOption
    values.foldLeft(z)((acc: Option[Interval[T]], i: Interval[T]) => acc.flatMap(_ union i))
  }
}
