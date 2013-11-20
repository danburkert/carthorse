package scalabase

import org.scalacheck.Arbitrary
import scalabase.async.Deferred

/**
 * Defines ScalaCheck generators for property testing.
 */
trait Generators {
  def genSuccess[T](implicit v: Arbitrary[T]) = for(e <- Arbitrary.arbitrary[T]) yield Deferred.successful(e)
  def genFailure[T] = for(e <- Arbitrary.arbitrary[String]) yield Deferred.failed[T](new RuntimeException(e))

  implicit def arbitraryDeferred[T](implicit v: Arbitrary[T]): Arbitrary[Deferred[T]] = Arbitrary {
    genSuccess[T] | genFailure[T]
  }
}
