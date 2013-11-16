package scalabase.structures

import scala.collection.immutable.SortedSet
import scala.collection.SetLike

/**
 * A set of non-overlapping `Interval`s over the space of an [[scala.math.Ordered]]
 * type.
 */
case class IntervalSet[T <% Ordered[T]] private(override val seq: SortedSet[Interval[T]])
  extends Set[Interval[T]] with SetLike[Interval[T], IntervalSet[T]] {

  override def empty: IntervalSet[T] = IntervalSet(SortedSet.empty[Interval[T]])

  override def iterator: Iterator[Interval[T]] = seq.iterator

  override def size: Int = seq.size

  override def foreach[U](f: (Interval[T]) => U): Unit = seq.foreach(f)

  /**
   * Return the set of `Interval`s which successfully union with the given `Interval`.
   *
   * @param i `Interval` to check for unions with.
   * @return `Interval` in set which can be unioned with i.
   */
  protected def unions(i: Interval[T]): SortedSet[Interval[T]] = {
    if (seq.isEmpty) return SortedSet()

    val leftMost = seq.firstKey.start
    val rightMost = seq.lastKey.end

    if (i.start > rightMost || i.end < leftMost) return SortedSet()

    // Drop all intervals to the left of i
    (if (i.start <= leftMost) seq else seq.from(Interval(leftMost, i.start)))
        .takeWhile(_.start <= i.end) // Drop all intervals to the right of i
  }

  /**
   * Return the set of `Interval`s which successfully intersect with the given `Interval`.
   *
   * @param i `Interval` to check for intersections with.
   * @return `Interval` in set which can be intersected with i.
   */
  protected def intersects(i: Interval[T]): SortedSet[Interval[T]] = {
    if (seq.isEmpty) return SortedSet()

    val leftMost = seq.firstKey.start
    val rightMost = seq.lastKey.end

    if (i.start >= rightMost || i.end <= leftMost) return SortedSet()

    // Drop all intervals to the left of i
    (if (i.start <= leftMost) seq else seq.from(Interval(leftMost, i.start)))
        .dropWhile(_.end <= i.start) // Drop tangent interval on left
        .takeWhile(_.start < i.end) // Drop all intervals to the right of i
  }

  override def contains(elem: Interval[T]): Boolean = {
    val i = intersects(elem)
    i.size == 1 && i.head.start <= elem.start && i.head.end >= elem.end
  }

  override def +(elem: Interval[T]): IntervalSet[T] = {
    val unionables = unions(elem)
    val union = unionables.foldLeft(elem) { (a: Interval[T], b: Interval[T]) => a.union(b).get }
    IntervalSet(seq -- unionables + union)
  }

  override def -(elem: Interval[T]): IntervalSet[T] = {
    val intersections = intersects(elem)
    val differences = intersections.flatMap(_.difference(elem))
    IntervalSet(seq -- intersections ++ differences)
  }

  override val stringPrefix: String = "IntervalSet"
}

object IntervalSet {
  /**
   * Provides an implicit conversion between a 2-tuple of an `Ordered` type and a single element
   * `IntervalSet`.
   *
   * @param tuple 2-tuple to convert to an `Interval` and wrap in an `IntervalSet`.
   * @tparam T type of elements in the 2-tuple and resulting `Interval` in the set.
   * @return `IntervalSet` consisting of the single `Interval` specified by the tuple.
   */
  implicit def tupleToIntervalSet[T <% Ordered[T]](tuple: (T, T)): IntervalSet[T] =
      IntervalSet(Interval(tuple._1, tuple._2))

  /**
   * Provides an implicit conversion between an `Interval` and an `IntervalSet` containing the
   * single `Interval`.
   *
   * @param interval to wrap in an `IntervalSet`.
   * @tparam T type of elements in `Interval`.
   * @return `IntervalSet` consisting of the single `Interval`.
   */
  implicit def IntervalToIntervalSet[T <% Ordered[T]](interval: Interval[T]): IntervalSet[T] =
      IntervalSet(interval)

  /**
   * Returns a new empty `IntervalSet`.
   *
   * @tparam T type held by the `Interval`s in the set.
   * @return An empty `IntervalSet`.
   */
  def empty[T <% Ordered[T]]: IntervalSet[T] = IntervalSet(SortedSet.empty[Interval[T]])

  /**
   * Returns a new `IntervalSet` containing the supplied `Interval`.
   *
   * @param interval to be held by the new `IntervalSet`.
   * @tparam T type held be the `Interval`s in the set.
   * @return a new `IntervalSet` containing the supplied `Interval`.
   */
  def apply[T <% Ordered[T]](interval: Interval[T]): IntervalSet[T] =
      new IntervalSet(SortedSet.empty[Interval[T]] + interval)

  /**
   * Returns a new `IntervalSet` containing the union of the supplied `Interval`s.
   *
   * Note: this method attempts to sort the supplied `Interval`s, so use caution when passing a
   *       lazily evaluated collection or iterator.
   *
   * @param intervals to be held by the new `IntervalSet`.
   * @tparam T type held by the `Interval`s in the set.
   * @return a new `IntervalSet` containing the union of the supplied `Interval`s.
   */
  def union[T <% Ordered[T]](intervals: TraversableOnce[Interval[T]]): IntervalSet[T] = {
    val union = intervals
        .toSeq
        .sorted
        .foldLeft(List[Interval[T]]()) { (acc: List[Interval[T]], right: Interval[T]) =>
          acc match {
            case Nil => right :: Nil
            case left :: rest => right.union(left).map(rest.::).getOrElse(right :: left :: rest)
          }
        }
    new IntervalSet(SortedSet.empty[Interval[T]] ++ union)
  }

  /**
   * Returns a new `IntervalSet` containing the union of the supplied `Interval`s.
   *
   * @param intervals to be held by the new `IntervalSet`.
   * @tparam T type held by the `Interval`s in the set.
   * @return a new `IntervalSet` containing the union of the supplied `Interval`s.
   */
  def union[T <% Ordered[T]](intervals: Interval[T]*): IntervalSet[T] = union(intervals.toList)

  /**
   * Returns a new `IntervalSet` containing the intersection of the supplied `Interval`s.
   *
   * @param intervals to be held by the new `IntervalSet`.
   * @tparam T type held by the `Interval`s in the set.
   * @return a new `IntervalSet` containing the union of the supplied `Interval`s.
   */
  def intersect[T <% Ordered[T]](intervals: TraversableOnce[Interval[T]]): IntervalSet[T] = {
    val z = intervals.toStream.headOption
    intervals
        .foldLeft(z) { (acc: Option[Interval[T]], i: Interval[T]) => acc.flatMap(_.intersect(i)) }
        .map(IntervalSet(_))
        .getOrElse(empty)
  }

  /**
   * Returns a new `IntervalSet` containing the intersection of the supplied `Interval`s.
   *
   * @param intervals to be held by the new `IntervalSet`.
   * @tparam T type held by the `Interval`s in the set.
   * @return a new `IntervalSet` containing the union of the supplied `Interval`s.
   */
  def intersect[T <% Ordered[T]](intervals: Interval[T]*): IntervalSet[T] = intersect(intervals)
}
