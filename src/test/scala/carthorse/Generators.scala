package carthorse

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import java.util.UUID

trait Generators extends continuum.test.Generators {

  implicit def arbRichBytes: Arbitrary[RichBytes] =
    Arbitrary(for (bytes <- arbitrary[Array[Byte]]) yield new RichBytes(bytes))

  implicit def arbUUID: Arbitrary[UUID] =
    Arbitrary(UUID.randomUUID())
}
