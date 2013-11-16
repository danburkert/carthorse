package scalabase.structures

/**
 * A continuous interval over the space of an `Ordered` type.  The start of the interval is
 * inclusive, and the end is exclusive, that is, [start, end).
 *
 * @param start inclusive start of `Interval`.
 * @param end exclusive end of `Interval`.
 * @tparam T type of elements in the `Interval`.
 */
case class Interval[T <% Ordered[T]](start: T, end: T) extends Ordered[Interval[T]] {
  require(start != null, "the start of an interval may not be null.")
  require(end != null, "the end of an interval may not be null.")
  require(start < end, "the start of an interval must be less than the end.")

  /**
   * Test if this interval contains a given point.
   *
   * @param point to test on interval.
   * @return whether the interval contains the point.
   */
  def contains(point: T): Boolean = point >= start && point < end

  /**
   * Test whether another interval intersects this one.
   *
   * @param other interval to test for an intersection.
   * @return whether the other interval intersects this one.
   */
  def intersects(other: Interval[T]): Boolean = {
    start < other.end && other.start < end
  }

  /**
   * Test whether another interval can successfully union this one.
   *
   * @param other interval to test for union.
   * @return whether the other interval can successfully union this one.
   */
  def unions(other: Interval[T]): Boolean = {
    start <= other.end && other.start <= end
  }

  /**
   * Returns the intersection of this `Interval` and another.
   *
   * @param other interval to return intersection of.
   * @return the intersection of the the other interval and this one.
   */
  def intersect(other: Interval[T]): Option[Interval[T]] = {
    if (intersects(other)) {
      val s = if (start < other.start) other.start else start
      val e = if (end > other.end) other.end else end
      Some(Interval(s, e))
    } else {
      None
    }
  }

  /**
   * Returns the contiguous union of this interval and another as an `Interval`, if possible.  Use
   * `#unions` to test whether two `Interval`s may be successfully unioned.
   *
   * @param other Interval to be unioned with.
   * @return the union of this interval and the other.
   */
  def union(other: Interval[T]): Option[Interval[T]] = {
    if (unions(other)) {
      val s = if (start < other.start) start else other.start
      val e = if (end > other.end) end else other.end
      Some(Interval(s, e))
    } else {
      None
    }
  }

  /**
   * Returns the difference between this `Interval` and the other as a `Seq` of `Interval`s.
   *
   * @param other `Interval` to subtract from `this`.
   * @return the interval difference between `this` and `that`.  As a sequence of `Interval`s.
   */
  def difference(other: Interval[T]): Seq[Interval[T]] = {
    if (intersects(other)) {
      if (start < other.start && end > other.end) {
        List(start -> other.start, other.end -> end)
      } else if (start >= other.start && end <= other.end) {
        List()
      } else {
        val l = if (start < other.start) start else other.end
        val r = if (end <= other.end) other.start else end
        List(l -> r)
      }
    } else {
      List(this)
    }
  }


  /**
   * Compares this `Interval` to another by the end point, and then the start point.
   *
   * @param that `Interval` to compare to.
   * @return relative ordering of the intervals.
   */
  def compare(that: Interval[T]): Int = {
    val e = this.end.compare(that.end)
    if (e != 0) e else this.start.compare(that.start)
  }
}

object Interval {
  /**
   * Provides an implicit conversion between a 2-tuple of an `Ordered` type and an `Interval`.
   *
   * @param t 2-tuple to convert to an interval.
   * @tparam T type of elements in the 2-tuple and resulting interval.
   * @return interval consisting of the start and end point contained in the tuple.
   */
  implicit def tupleToInterval[T <% Ordered[T]](t: (T, T)): Interval[T] = Interval(t._1, t._2)
}
