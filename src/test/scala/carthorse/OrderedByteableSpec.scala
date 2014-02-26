package carthorse

import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import java.util.UUID

class OrderedByteableSpec extends
  PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with Generators {

  property("Native UUID sort and OrderedByteable's asc UUID bytes sort are equivalent") {
    forAll { (a: UUID, b: UUID) =>
      Integer.signum(a.compareTo(b)) should equal (Integer.signum(OrderedByteable.toBytesAsc(a) compare OrderedByteable.toBytesAsc(b)))
    }
  }

  property("Native UUID sort and OrderedByteable's desc UUID bytes sort are equivalent") {
    forAll { (a: UUID, b: UUID) =>
      Integer.signum(a.compareTo(b)) should equal (Integer.signum(OrderedByteable.toBytesDesc(b) compare OrderedByteable.toBytesDesc(a)))
    }
  }
}
