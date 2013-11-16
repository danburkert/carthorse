package scalabase.structures

import org.scalatest.FunSuite

class IntervalSuite extends FunSuite {

  test("An interval can not be created with a start point greater than its end point.") {
    intercept[IllegalArgumentException] {
      Interval(20, 10)
    }
  }

  test("An interval can not be created with a start point equal to its end point.") {
    intercept[IllegalArgumentException] {
      Interval(10, 10)
    }
  }

  test("An interval can not be created with a null start point.") {
    intercept[IllegalArgumentException] {
      Interval(null, "end")
    }
  }

  test("An interval can not be created with a null end point.") {
    intercept[IllegalArgumentException] {
      Interval("start", null)
    }
  }

  test("A tuple can be implicitly converted to an Interval.") {
    val i: Interval[Int] = (1, 2)
    assert(Interval(1, 2) === i)
  }

  /**
   *    [-------------------------)
   *            *
   */
  test("An interval should contain a point between its start and end points.") {
    val interval = Interval(10, 20)
    assert(true === interval.contains(15))
  }

  /**
   *    [-------------------------)
   *    *
   */
  test("An interval should contain a point equal to its start point.") {
    val interval = Interval(10, 20)
    assert(true === interval.contains(10))
  }

  /**
   *    [-------------------------)
   *                              *
   */
  test("An interval should not contain a point equal to its end point.") {
    val interval = Interval(10, 20)
    assert(false === interval.contains(20))
  }


  /**
   *    [-------------------------)
   *  *
   */
  test("An interval should not contain a point less than its start point.") {
    val interval = Interval(10, 20)
    assert(false === interval.contains(8))
  }

  /**
   *    [-------------------------)
   *                                  *
   */
  test("An interval should not contain a point greater than its end point.") {
    val interval = Interval(10, 20)
    assert(false === interval.contains(25))
  }

  /**
   * left       [-----------------)
   * right                               [-----------------)
   */
  test("Non-overlapping intervals.") {
    val left = Interval(1, 10)
    val right = Interval(20, 30)
    assert(false === left.intersects(right))
    assert(false === right.intersects(left))
    assert(false === left.unions(right))
    assert(false === right.unions(left))
    assert(None === left.intersect(right))
    assert(None === right.intersect(left))
    assert(None === left.union(right))
    assert(None == right.union(left))
    assert(left < right)
    assert(right > left)
  }

  /**
   * left           [-----------------)
   * right                     [-----------------)
   * intersection              [------)
   * union          [----------------------------)
   */
  test("Partially overlapping intervals.") {
    val left = Interval(1, 10)
    val right = Interval(5, 15)
    val intersection = Interval(5, 10)
    val union = Interval(1, 15)
    assert(true === left.intersects(right))
    assert(true === right.intersects(left))
    assert(true === left.unions(right))
    assert(true === right.unions(left))
    assert(intersection === left.intersect(right).get)
    assert(intersection === right.intersect(left).get)
    assert(union === left.union(right).get)
    assert(union === right.union(left).get)
    assert(List(Interval(1, 5)) === left.difference(right))
    assert(List(Interval(10, 15)) === right.difference(left))
    assert(left < right)
    assert(right > left)
  }

  /**
   * bigger           [-----------------------------)
   * smaller                [-----------------)
   * intersection           [-----------------)
   * union            [-----------------------------)
   */
  test("Fully overlapping intervals.") {
    val bigger = Interval(1, 10)
    val smaller = Interval(3, 7)
    val intersection = smaller
    val union = bigger
    assert(true === bigger.intersects(smaller))
    assert(true === smaller.intersects(bigger))
    assert(true === bigger.unions(smaller))
    assert(true === smaller.unions(bigger))
    assert(intersection === bigger.intersect(smaller).get)
    assert(intersection === smaller.intersect(bigger).get)
    assert(union === bigger.union(smaller).get)
    assert(union === smaller.union(bigger).get)
    assert(List(Interval(1, 3), Interval(7, 10)) === bigger.difference(smaller))
    assert(List() === smaller.difference(bigger))
    assert(bigger > smaller)
    assert(smaller < bigger)
  }

  /**
   * left           [------------)
   * right                       [-----------------)
   * union          [------------------------------)
   */
  test("Tangent intervals.") {
    val left = Interval(1, 10)
    val right = Interval(10, 17)
    assert(false === left.intersects(right))
    assert(false === right.intersects(left))
    assert(true === left.unions(right))
    assert(true === right.unions(left))
    assert(None === left.intersect(right))
    assert(None === right.intersect(left))
    assert(Some(Interval(1, 17)) === left.union(right))
    assert(None === right.intersect(left))
    assert(List(left) === left.difference(right))
    assert(List(right) === right.difference(left))
    assert(left < right)
    assert(right > left)
  }

  /**
   * bigger       [----------------)
   * smaller      [-----------)
   * intersection [-----------)
   * union        [----------------)
   */
  test("Equal start point intervals.") {
    val bigger = Interval(1, 10)
    val smaller = Interval(1, 5)
    assert(true === bigger.intersects(smaller))
    assert(true === smaller.intersects(bigger))
    assert(true === bigger.unions(smaller))
    assert(true === smaller.unions(bigger))
    assert(Some(smaller) === bigger.intersect(smaller))
    assert(Some(smaller) === smaller.intersect(bigger))
    assert(Some(bigger) === bigger.union(smaller))
    assert(Some(bigger) === smaller.union(bigger))
    assert(List(Interval(5, 10)) === bigger.difference(smaller))
    assert(List() === smaller.difference(bigger))
    assert(bigger > smaller)
    assert(smaller < bigger)
  }

  /**
   * bigger       [----------------)
   * smaller           [-----------)
   * intersection      [-----------)
   * union        [----------------)
   */
  test("Equal end point intervals.") {
    val bigger = Interval(1, 10)
    val smaller = Interval(5, 10)
    assert(true === bigger.intersects(smaller))
    assert(true === smaller.intersects(bigger))
    assert(true === bigger.unions(smaller))
    assert(true === smaller.unions(bigger))
    assert(Some(smaller) === bigger.intersect(smaller))
    assert(Some(smaller) === smaller.intersect(bigger))
    assert(Some(bigger) === bigger.union(smaller))
    assert(Some(bigger) === smaller.union(bigger))
    assert(List(Interval(1, 5)) === bigger.difference(smaller))
    assert(List() === smaller.difference(bigger))
    assert(bigger < smaller)
    assert(smaller > bigger)
  }

  /**
   * bigger         [----------------)
   * smaller        [----------------)
   * intersection   [----------------)
   * union          [----------------)
   */
  test("Equal intervals.") {
    val interval = Interval(1, 10)
    assert(true === interval.intersects(interval))
    assert(true === interval.unions(interval))
    assert(Some(interval) === interval.intersect(interval))
    assert(Some(interval) === interval.union(interval))
    assert(interval <= interval)
    assert(interval >= interval)
    assert(interval == interval)
    assert(List() === interval.difference(interval))
    assert(!(interval < interval))
    assert(!(interval > interval))
  }
}
