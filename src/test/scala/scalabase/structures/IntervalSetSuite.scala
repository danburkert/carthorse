package scalabase.structures

import org.scalatest.FunSuite

class IntervalSetSuite extends FunSuite {

  test("A tuple can be implicitly converted to an IntervalSet.") {
    val i: IntervalSet[Int] = (1, 2)
    assert(IntervalSet(Interval(1, 2)) === i)
  }

  test("An Interval can be implicitly converted to an IntervalSet.") {
    val i: IntervalSet[Int] = Interval(1, 2)
    assert(IntervalSet(Interval(1, 2)) === i)
  }

  test("Empty set.") {
    val empty = IntervalSet.empty[Int]
    val union = IntervalSet.union[Int]()
    val intersection = IntervalSet.intersect[Int]()

    assert(0 === empty.size)
    assert(0 === union.size)
    assert(0 === intersection.size)
    assert(Set[Interval[Int]]() === empty.seq)
    assert(Set[Interval[Int]]() === union.seq)
    assert(Set[Interval[Int]]() === intersection.seq)
  }

  test("Single interval set.") {
    val interval = Interval(1, 2)
    val single = IntervalSet(interval)
    val union = IntervalSet.union(interval)
    val intersection = IntervalSet.intersect(interval)

    assert(1 === single.size)
    assert(1 === union.size)
    assert(1 === intersection.size)
    assert(Set(interval) === single.seq)
    assert(Set(interval) === union.seq)
    assert(Set(interval) === intersection.seq)
  }

  /**
   *  a                       [--------------)
   *  b       [---------)
   *  c                   [--)
   *  d                                            [---------)
   */
  test("Disjoint intervals.") {
    val a = 10 -> 20
    val b = 1 -> 5
    val c = 7 -> 9
    val d = 25 -> 30
    val union = IntervalSet.union(a, b, c, d)
    val intersection = IntervalSet.intersect(a, b, c, d)

    assert(4 === union.size)
    assert(0 === intersection.size)
    assert(Set[Interval[Int]](a, b, c, d) === union.seq)
    assert(Set[Interval[Int]]() === intersection.seq)
  }

  /**
   * a                [--------------)
   * b           [-------------)
   * c                      [------------)
   * d      [------)
   */
  test("Partially overlapping intervals.") {
    val a = 15 -> 25
    val b = 8 -> 20
    val c = 18 -> 30
    val d = 5 -> 10
    val union = IntervalSet.union(a, b, c, d)
    val intersection = IntervalSet.intersect(a, b, c, d)

    assert(1 === union.size)
    assert(0 === intersection.size)
    assert(Set[Interval[Int]](5 -> 30) === union.seq)
    assert(Set[Interval[Int]]() === intersection.seq)
  }

  /**
   * a                [--------------)
   * b           [-------------)
   * c                      [------------)
   * d      [----------------)
   */
  test("Partially overlapping intervals with union.") {
    val a = 15 -> 25
    val b = 10 -> 20
    val c = 18 -> 30
    val d = 5 -> 19
    val union = IntervalSet.union(a, b, c, d)
    val intersection = IntervalSet.intersect(a, b, c, d)

    assert(1 === union.size)
    assert(1 === intersection.size)
    assert(Set[Interval[Int]](5 -> 30) === union.seq)
    assert(Set[Interval[Int]](18 -> 19) === intersection.seq)
  }

  /**
   *  a          [--------------)
   *  b   [----------------------------)
   *  c   [------)
   *  d                      [----)
   */
  test("Fully overlapping intervals.") {
    val a = 5 -> 15
    val b = 0 -> 20
    val c = 0 -> 5
    val d = 12 -> 18
    val union = IntervalSet.union(a, b, c, d)
    val intersection = IntervalSet.intersect(a, b, c, d)

    assert(1 === union.size)
    assert(0 === intersection.size)
    assert(union.seq === Set[Interval[Int]](b))
    assert(intersection.seq === Set.empty)
  }

  /**
   *  a          [--------------)
   *  b   [----------------------------)
   *  c            [---------)
   *  d             [----)
   */
  test("Fully overlapping intervals with union.") {
    val a = 10 -> 20
    val b = 0 -> 30
    val c = 12 -> 18
    val d = 14 -> 16
    val union = IntervalSet.union(a, b, c, d)
    val intersection = IntervalSet.intersect(a, b, c, d)

    assert(1 === union.size)
    assert(1 === intersection.size)
    assert(union.seq === Set[Interval[Int]](b))
    assert(intersection.seq === Set[Interval[Int]](d))
  }

  /**
   *  a              [--------------)
   *  b       [------)
   *  c                             [-----)
   *  d  [----)
   */
  test("Tangent intervals.") {
    val a = 10 -> 15
    val b = 5 -> 10
    val c = 15 -> 20
    val d = 0 -> 5
    val union = IntervalSet.union(a, b, c, d)
    val intersection = IntervalSet.intersect(a, b, c, d)

    assert(1 === union.size)
    assert(0 === intersection.size)
    assert(Set[Interval[Int]](0 -> 20) === union.seq)
    assert(Set[Interval[Int]]() === intersection.seq)
  }

  /**
   *  a                   [--------------)
   *  b       [------)
   *  c                             [-----)
   *  d  [----)
   */
  test("Partially overlapping interval groups.") {
    val a = 15 -> 24
    val b = 20 -> 25
    val c = 8 -> 12
    val d = 2 -> 8
    val union = IntervalSet.union(a, b, c, d)
    val intersection = IntervalSet.intersect(a, b, c, d)

    assert(2 === union.size)
    assert(0 === intersection.size)
    assert(Set[Interval[Int]](15 -> 25, 2 -> 12) === union.seq)
    assert(Set[Interval[Int]]() === intersection.seq)
  }

  /**
   *  a       [--------------)
   *  b       [--------------)
   *  c       [--------------)
   *  d       [--------------)
   */
  test("Equivalent intervals.") {
    val a = Interval(10, 20)
    val b = a
    val c = a
    val d = a
    val union = IntervalSet.union(a, b, c, d)
    val intersection = IntervalSet.intersect(a, b, c, d)

    assert(1 === union.size)
    assert(1 === intersection.size)
    assert(Set[Interval[Int]](a) === union.seq)
    assert(Set[Interval[Int]](a) === intersection.seq)
  }

  /**
   *         a    b    c    d    e    f    g    h    i    j    k
   * left              [---------)
   * right                                 [---------)
   */
  test("IntervalSet can add an interval.") {
    val a = 0
    val b = 5
    val c = 10
    val d = 15
    val e = 20
    val f = 25
    val g = 30
    val h = 35
    val i = 40
    val j = 45
    val k = 50

    val left: Interval[Int] = c -> e
    val right: Interval[Int] = g -> i

    // a -> every other point
    assert(Set[Interval[Int]](a -> b, left, right) === (IntervalSet.union(left, right) + (a -> b)).seq)
    assert(Set[Interval[Int]](a -> e, right) === (IntervalSet.union(left, right) + (a -> c)).seq)
    assert(Set[Interval[Int]](a -> e, right) === (IntervalSet.union(left, right) + (a -> d)).seq)
    assert(Set[Interval[Int]](a -> e, right) === (IntervalSet.union(left, right) + (a -> e)).seq)
    assert(Set[Interval[Int]](a -> f, right) === (IntervalSet.union(left, right) + (a -> f)).seq)
    assert(Set[Interval[Int]](a -> i) === (IntervalSet.union(left, right) + (a -> g)).seq)
    assert(Set[Interval[Int]](a -> i) === (IntervalSet.union(left, right) + (a -> h)).seq)
    assert(Set[Interval[Int]](a -> i) === (IntervalSet.union(left, right) + (a -> i)).seq)
    assert(Set[Interval[Int]](a -> j) === (IntervalSet.union(left, right) + (a -> j)).seq)
    assert(Set[Interval[Int]](a -> k) === (IntervalSet.union(left, right) + (a -> k)).seq)

    // b -> every other point
    assert(Set[Interval[Int]](b -> e, right) === (IntervalSet.union(left, right) + (b -> c)).seq)
    assert(Set[Interval[Int]](b -> e, right) === (IntervalSet.union(left, right) + (b -> d)).seq)
    assert(Set[Interval[Int]](b -> e, right) === (IntervalSet.union(left, right) + (b -> e)).seq)
    assert(Set[Interval[Int]](b -> f, right) === (IntervalSet.union(left, right) + (b -> f)).seq)
    assert(Set[Interval[Int]](b -> i) === (IntervalSet.union(left, right) + (b -> g)).seq)
    assert(Set[Interval[Int]](b -> i) === (IntervalSet.union(left, right) + (b -> h)).seq)
    assert(Set[Interval[Int]](b -> i) === (IntervalSet.union(left, right) + (b -> i)).seq)
    assert(Set[Interval[Int]](b -> j) === (IntervalSet.union(left, right) + (b -> j)).seq)
    assert(Set[Interval[Int]](b -> k) === (IntervalSet.union(left, right) + (b -> k)).seq)

    // c -> every other point
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) + (c -> d)).seq)
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) + (c -> e)).seq)
    assert(Set[Interval[Int]](c -> f, right) === (IntervalSet.union(left, right) + (c -> f)).seq)
    assert(Set[Interval[Int]](c -> i) === (IntervalSet.union(left, right) + (c -> g)).seq)
    assert(Set[Interval[Int]](c -> i) === (IntervalSet.union(left, right) + (c -> h)).seq)
    assert(Set[Interval[Int]](c -> i) === (IntervalSet.union(left, right) + (c -> i)).seq)
    assert(Set[Interval[Int]](c -> j) === (IntervalSet.union(left, right) + (c -> j)).seq)
    assert(Set[Interval[Int]](c -> k) === (IntervalSet.union(left, right) + (c -> k)).seq)

    // d -> every other point
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) + (d -> e)).seq)
    assert(Set[Interval[Int]](c -> f, right) === (IntervalSet.union(left, right) + (d -> f)).seq)
    assert(Set[Interval[Int]](c -> i) === (IntervalSet.union(left, right) + (d -> g)).seq)
    assert(Set[Interval[Int]](c -> i) === (IntervalSet.union(left, right) + (d -> h)).seq)
    assert(Set[Interval[Int]](c -> i) === (IntervalSet.union(left, right) + (d -> i)).seq)
    assert(Set[Interval[Int]](c -> j) === (IntervalSet.union(left, right) + (d -> j)).seq)
    assert(Set[Interval[Int]](c -> k) === (IntervalSet.union(left, right) + (d -> k)).seq)

    // e -> every other point
    assert(Set[Interval[Int]](c -> f, right) === (IntervalSet.union(left, right) + (e -> f)).seq)
    assert(Set[Interval[Int]](c -> i) === (IntervalSet.union(left, right) + (e -> g)).seq)
    assert(Set[Interval[Int]](c -> i) === (IntervalSet.union(left, right) + (e -> h)).seq)
    assert(Set[Interval[Int]](c -> i) === (IntervalSet.union(left, right) + (e -> i)).seq)
    assert(Set[Interval[Int]](c -> j) === (IntervalSet.union(left, right) + (e -> j)).seq)
    assert(Set[Interval[Int]](c -> k) === (IntervalSet.union(left, right) + (e -> k)).seq)

    // f -> every other point
    assert(Set[Interval[Int]](left, f -> i) === (IntervalSet.union(left, right) + (f -> g)).seq)
    assert(Set[Interval[Int]](left, f -> i) === (IntervalSet.union(left, right) + (f -> h)).seq)
    assert(Set[Interval[Int]](left, f -> i) === (IntervalSet.union(left, right) + (f -> i)).seq)
    assert(Set[Interval[Int]](left, f -> j) === (IntervalSet.union(left, right) + (f -> j)).seq)
    assert(Set[Interval[Int]](left, f -> k) === (IntervalSet.union(left, right) + (f -> k)).seq)

    // g -> every other point
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) + (g -> h)).seq)
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) + (g -> i)).seq)
    assert(Set[Interval[Int]](left, g -> j) === (IntervalSet.union(left, right) + (g -> j)).seq)
    assert(Set[Interval[Int]](left, g -> k) === (IntervalSet.union(left, right) + (g -> k)).seq)

    // h -> every other point
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) + (h -> i)).seq)
    assert(Set[Interval[Int]](left, g -> j) === (IntervalSet.union(left, right) + (h -> j)).seq)
    assert(Set[Interval[Int]](left, g -> k) === (IntervalSet.union(left, right) + (h -> k)).seq)

    // i -> every other point
    assert(Set[Interval[Int]](left, g -> j) === (IntervalSet.union(left, right) + (i -> j)).seq)
    assert(Set[Interval[Int]](left, g -> k) === (IntervalSet.union(left, right) + (i -> k)).seq)

    // j -> every other point
    assert(Set[Interval[Int]](left, right, j -> k) === (IntervalSet.union(left, right) + (j -> k)).seq)
  }

  /**
   *         a    b    c    d    e    f    g    h    i    j    k
   * left              [---------)
   * right                                 [---------)
   */
  test("IntervalSet can remove an interval.") {
    val a = 0
    val b = 5
    val c = 10
    val d = 15
    val e = 20
    val f = 25
    val g = 30
    val h = 35
    val i = 40
    val j = 45
    val k = 50

    val left: Interval[Int] = c -> e
    val right: Interval[Int] = g -> i

    // a -> every other point
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) - (a -> b)).seq)
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) - (a -> c)).seq)
    assert(Set[Interval[Int]](d -> e, right) === (IntervalSet.union(left, right) - (a -> d)).seq)
    assert(Set[Interval[Int]](right) === (IntervalSet.union(left, right) - (a -> e)).seq)
    assert(Set[Interval[Int]](right) === (IntervalSet.union(left, right) - (a -> f)).seq)
    assert(Set[Interval[Int]](right) === (IntervalSet.union(left, right) - (a -> g)).seq)
    assert(Set[Interval[Int]](h -> i) === (IntervalSet.union(left, right) - (a -> h)).seq)
    assert(Set[Interval[Int]]() === (IntervalSet.union(left, right) - (a -> i)).seq)
    assert(Set[Interval[Int]]() === (IntervalSet.union(left, right) - (a -> j)).seq)
    assert(Set[Interval[Int]]() === (IntervalSet.union(left, right) - (a -> k)).seq)

    // b -> every other point
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) - (b -> c)).seq)
    assert(Set[Interval[Int]](d -> e, right) === (IntervalSet.union(left, right) - (b -> d)).seq)
    assert(Set[Interval[Int]](right) === (IntervalSet.union(left, right) - (b -> e)).seq)
    assert(Set[Interval[Int]](right) === (IntervalSet.union(left, right) - (b -> f)).seq)
    assert(Set[Interval[Int]](right) === (IntervalSet.union(left, right) - (b -> g)).seq)
    assert(Set[Interval[Int]](h -> i) === (IntervalSet.union(left, right) - (b -> h)).seq)
    assert(Set[Interval[Int]]() === (IntervalSet.union(left, right) - (b -> i)).seq)
    assert(Set[Interval[Int]]() === (IntervalSet.union(left, right) - (b -> j)).seq)
    assert(Set[Interval[Int]]() === (IntervalSet.union(left, right) - (b -> k)).seq)

    // c -> every other point
    assert(Set[Interval[Int]](d -> e, right) === (IntervalSet.union(left, right) - (c -> d)).seq)
    assert(Set[Interval[Int]](right) === (IntervalSet.union(left, right) - (c -> e)).seq)
    assert(Set[Interval[Int]](right) === (IntervalSet.union(left, right) - (c -> f)).seq)
    assert(Set[Interval[Int]](right) === (IntervalSet.union(left, right) - (c -> g)).seq)
    assert(Set[Interval[Int]](h -> i) === (IntervalSet.union(left, right) - (c -> h)).seq)
    assert(Set[Interval[Int]]() === (IntervalSet.union(left, right) - (c -> i)).seq)
    assert(Set[Interval[Int]]() === (IntervalSet.union(left, right) - (c -> j)).seq)
    assert(Set[Interval[Int]]() === (IntervalSet.union(left, right) - (c -> k)).seq)

    // d -> every other point
    assert(Set[Interval[Int]](c -> d, right) === (IntervalSet.union(left, right) - (d -> e)).seq)
    assert(Set[Interval[Int]](c -> d, right) === (IntervalSet.union(left, right) - (d -> f)).seq)
    assert(Set[Interval[Int]](c -> d, right) === (IntervalSet.union(left, right) - (d -> g)).seq)
    assert(Set[Interval[Int]](c -> d, h -> i) === (IntervalSet.union(left, right) - (d -> h)).seq)
    assert(Set[Interval[Int]](c -> d) === (IntervalSet.union(left, right) - (d -> i)).seq)
    assert(Set[Interval[Int]](c -> d) === (IntervalSet.union(left, right) - (d -> j)).seq)
    assert(Set[Interval[Int]](c -> d) === (IntervalSet.union(left, right) - (d -> k)).seq)

    // e -> every other point
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) - (e -> f)).seq)
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) - (e -> g)).seq)
    assert(Set[Interval[Int]](left, h -> i) === (IntervalSet.union(left, right) - (e -> h)).seq)
    assert(Set[Interval[Int]](left) === (IntervalSet.union(left, right) - (e -> i)).seq)
    assert(Set[Interval[Int]](left) === (IntervalSet.union(left, right) - (e -> j)).seq)
    assert(Set[Interval[Int]](left) === (IntervalSet.union(left, right) - (e -> k)).seq)

    // f -> every other point
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) - (f -> g)).seq)
    assert(Set[Interval[Int]](left, h -> i) === (IntervalSet.union(left, right) - (f -> h)).seq)
    assert(Set[Interval[Int]](left) === (IntervalSet.union(left, right) - (f -> i)).seq)
    assert(Set[Interval[Int]](left) === (IntervalSet.union(left, right) - (f -> j)).seq)
    assert(Set[Interval[Int]](left) === (IntervalSet.union(left, right) - (f -> k)).seq)

    // g -> every other point
    assert(Set[Interval[Int]](left, h -> i) === (IntervalSet.union(left, right) - (g -> h)).seq)
    assert(Set[Interval[Int]](left) === (IntervalSet.union(left, right) - (g -> i)).seq)
    assert(Set[Interval[Int]](left) === (IntervalSet.union(left, right) - (g -> j)).seq)
    assert(Set[Interval[Int]](left) === (IntervalSet.union(left, right) - (g -> k)).seq)

    // h -> every other point
    assert(Set[Interval[Int]](left, g -> h) === (IntervalSet.union(left, right) - (h -> i)).seq)
    assert(Set[Interval[Int]](left, g -> h) === (IntervalSet.union(left, right) - (h -> j)).seq)
    assert(Set[Interval[Int]](left, g -> h) === (IntervalSet.union(left, right) - (h -> k)).seq)

    // i -> every other point
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) - (i -> j)).seq)
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) - (i -> k)).seq)

    // j -> every other point
    assert(Set[Interval[Int]](left, right) === (IntervalSet.union(left, right) - (j -> k)).seq)
  }

  /**
   *             a    b    c    d    e    f    g
   * interval              [---------)
   */
  test("IntervalSet can check for membership.") {
    val a = 0
    val b = 5
    val c = 10
    val d = 15
    val e = 20
    val f = 25
    val g = 30

    val set = IntervalSet(c -> e)

    // a -> every other point
    assert(false === set.contains(a -> b))
    assert(false === set.contains(a -> c))
    assert(false === set.contains(a -> d))
    assert(false === set.contains(a -> e))
    assert(false === set.contains(a -> f))
    assert(false === set.contains(a -> g))

    // b -> every other point
    assert(false === set.contains(b -> c))
    assert(false === set.contains(b -> d))
    assert(false === set.contains(b -> e))
    assert(false === set.contains(b -> f))
    assert(false === set.contains(b -> g))

    // c -> every other point
    assert(true === set.contains(c -> d))
    assert(true === set.contains(c -> e))
    assert(false === set.contains(c -> f))
    assert(false === set.contains(c -> g))

    // d -> every other point
    assert(true === set.contains(d -> e))
    assert(false === set.contains(d -> f))
    assert(false === set.contains(d -> g))

    // e -> every other point
    assert(false === set.contains(e -> f))
    assert(false === set.contains(e -> g))

    // f -> every other point
    assert(false === set.contains(f -> g))
  }
}
