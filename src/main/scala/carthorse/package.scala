import com.google.common.primitives.UnsignedBytes
import continuum.Discrete
import java.{util => ju}
import org.kiji.schema.{HBaseEntityId, EntityId}

package object carthorse {

  val Charset = java.nio.charset.Charset.forName("UTF-8")
  type RowKey = Identifier
  type Qualifier = Identifier

  implicit class Identifier(val bytes: Array[Byte]) extends AnyVal {
    override def toString: String = bytes.map("%02X" format _).mkString("Identifier(", ", ", ")")
  }

  /**
   * Provides implicit extensions for `Identifier` values, as well as utilty functions for working
   * with `Identifier`s.
   */
  implicit object Identifier
      extends Ordering[Identifier]
      with Discrete[Identifier] {

    override def next(value: Identifier): Option[Identifier] = {
      val ary = new Array[Byte](value.bytes.length + 1)
      value.bytes.copyToArray(ary)
      Some(ary)
    }

    override def compare(x: Identifier, y: Identifier): Int =
      UnsignedBytes.lexicographicalComparator().compare(x.bytes, y.bytes)

    def equals(x: Identifier, y: Identifier): Boolean = ju.Arrays.equals(x.bytes, y.bytes)

    def hashCode(identifier: Identifier): Int = ju.Arrays.hashCode(identifier.bytes)

    def minimum: Identifier = new Identifier(Array())
  }


  implicit object EntityIdImplicits extends Ordering[EntityId] with Discrete[EntityId] {

    implicit def entityIdToRowKey(entityId: EntityId): RowKey = entityId.getHBaseRowKey : RowKey
//    implicit def rowKeyToEntityId(rowKey: RowKey): EntityId = HBaseEntityId.fromHBaseRowKey(rowKey.bytes)

    def next(entityId: EntityId): Option[EntityId] =
      Identifier.next(entityId).map(rowkey => HBaseEntityId.fromHBaseRowKey(rowkey.bytes))

    def compare(x: EntityId, y: EntityId): Int = Identifier.compare(x, y)
  }
}
