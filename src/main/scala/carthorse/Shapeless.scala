package carthorse

import shapeless._

object Shapeless {

  object toSortableBytes extends Poly1 {
    implicit def caseInt = at[Int](x => 1)
    implicit def caseString = at[String](_.length)
    implicit def caseTuple[T, U](implicit st : Case.Aux[T, Int], su : Case.Aux[U, Int]) =
      at[(T, U)](t => toSortableBytes(t._1) + toSortableBytes(t._2))
  }

  object size extends Poly1 {
    implicit def caseInt = at[Int](x => 1)
    implicit def caseString = at[String](_.length)
    implicit def caseTuple[T, U]
    (implicit st : Case.Aux[T, Int], su : Case.Aux[U, Int]) =
      at[(T, U)](t => size(t._1)+size(t._2))
  }

//  object addSize extends Poly2 {
//    implicit  def default[T](implicit st: size.Case.Aux[T, Int]) =
//      at[Int, T]{ (acc, t) => acc + size(t) }
//  }

//  val l = 23 :: "foo" :: (13, "wibble") :: HNil
//
//  l.foldLeft(0)(addSize)
//
//  object myString extends Poly1 {
//    implicit def caseInt = at[Int](x => x.toString)
//    implicit def caseString = at[String](x => x)
//    implicit def caseTuple[T, U](implicit st: Case.Aux[T, String], su: Case.Aux[U, String]) =
//      at[(T, U)](t => "(" + myString(t._1) + ", " + myString(t._2) + ")")
//  }
//
//  object addString extends Poly2 {
//    implicit def default[T](implicit st: myString.Case.Aux[T, String]) =
//      at[String, T] { (acc, t) => acc + myString(t) }
//  }
//
//  Array.

}
