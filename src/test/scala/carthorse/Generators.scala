package carthorse

import org.scalacheck.Arbitrary

class Generators {

  def closedIntervalGen[T <: Ordered[T]](implicit a: Arbitrary[T]) = for {
    lower <- Arbitrary.arbitrary[Int]
    upper <- Arbitrary.arbitrary[Int] suchThat (n => n >= lower)
  } yield Interval.closed(lower, upper)

  def openIntervalGen[T <: Ordered[T]](implicit a: Arbitrary[T]) = for {
    lower <- Arbitrary.arbitrary[Int]
    upper <- Arbitrary.arbitrary[Int] suchThat (n => n >= lower)
  } yield Interval.closed(lower, upper)

}

object Generators {
  def main(args: Array[String]) {
    println("hello, fucker")
  }
}
