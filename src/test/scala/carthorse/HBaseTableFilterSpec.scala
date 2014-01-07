package carthorse

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec, BeforeAndAfterAll}

/**
 * Created by dan on 1/6/14.
 */
class HBaseTableFilterSpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with BeforeAndAfterAll
  with Generators {

  property("spores") {
    import scala.spores._
    import scala.pickling._
    import binary._

    val s = spore {
      (x: Int) => { x }
    }

    val p = s.pickle
    println(p)

    val f = p.unpickle[Spore[Int, Int]]

    println(f(123))
    println(s(3))
  }
}
