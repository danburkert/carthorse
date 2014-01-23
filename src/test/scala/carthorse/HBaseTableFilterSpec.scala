package carthorse

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec, BeforeAndAfterAll}

class HBaseTableFilterSpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with BeforeAndAfterAll
  with Generators {

  def foo(x: Int): Int = {
    x + 2
  }

  property("spores") {
//    import scala.spores._
    import scala.pickling._
    import json._

//    val s = spore {
//      foo _
//    }

    val f = (x: Int) => x + 42

//    val p = s.pickle
//    println(p)

    implicit class FunctionPickler[T, U](f: Function1[T, U])(implicit pf: PickleFormat) extends SPickler[Function1[T, U]] with Unpickler[Function1[T, U]]  {
      val format: PickleFormat = pf
      def pickle(picklee: (T) => U, builder: PBuilder): Unit =

      def unpickle(tag: => FastTypeTag[_], reader: PReader): Any = ???

    }

    println(implicitly[SPickler[Function1[Int, Int]]])
//    println(f.pickle)
//    println(f.pickle.unpickle[Function1[Int, Int]].apply(1))
//    println(s(3))
  }
}
