package carthorse

import java.{lang => jl}

import scala.language.implicitConversions

import org.apache.hadoop.hbase.util.{PositionedByteRange, Order, SimplePositionedByteRange}
import org.apache.hadoop.hbase.types._
import java.util.UUID
import com.google.common.primitives.{UnsignedLongs, UnsignedLong}

trait OrderedByteable[T] {
  type J <: AnyRef // Corresponding type to T in java API
  def ascDataType: DataType[J]
  def descDataType: DataType[J]
  implicit def s2j(s: T): J
  implicit def j2s(j: J): T
}

object OrderedByteable {

  def toBytesAsc[T](value: T)(implicit ev: OrderedByteable[T]): Array[Byte] = {
    import ev._
    val bytes = new SimplePositionedByteRange(ascDataType.encodedLength(value))
    ascDataType.encode(bytes, value)
    bytes.getBytes
  }

  def toBytesDesc[T](value: T)(implicit ev: OrderedByteable[T]): Array[Byte] = {
    import ev._
    val bytes = new SimplePositionedByteRange(descDataType.encodedLength(value))
    descDataType.encode(bytes, value)
    bytes.getBytes
  }

  def fromBytesAsc[T](bytes: Array[Byte])(implicit ev: OrderedByteable[T]): T = {
    import ev._
    ascDataType.decode(new SimplePositionedByteRange(bytes))
  }

  def fromBytesDesc[T](bytes: Array[Byte])(implicit ev: OrderedByteable[T]): T = {
    import ev._
    descDataType.decode(new SimplePositionedByteRange(bytes))
  }

  implicit object ByteOrderedByteable extends OrderedByteable[Byte] {
    type J = jl.Byte
    override val ascDataType: DataType[J] = OrderedInt8.ASCENDING
    override val descDataType: DataType[J] = OrderedInt8.DESCENDING
    override def s2j(s: Byte): J = s
    override def j2s(j: J): Byte = j.byteValue()
  }

  implicit object ShortOrderedByteable extends OrderedByteable[Short] {
    type J = jl.Short
    override val ascDataType: DataType[J] = OrderedInt16.ASCENDING
    override val descDataType: DataType[J] = OrderedInt16.DESCENDING
    override def s2j(s: Short): J = s
    override def j2s(j: J): Short = j.shortValue()
  }

  implicit object IntOrderedByteable extends OrderedByteable[Int] {
    type J = jl.Integer
    override val ascDataType: DataType[J] = OrderedInt32.ASCENDING
    override val descDataType: DataType[J] = OrderedInt32.DESCENDING
    override def s2j(s: Int): J = s
    override def j2s(j: J): Int = j.intValue()
  }

  implicit object LongOrderedByteable extends OrderedByteable[Long] {
    type J = jl.Long
    override val ascDataType: DataType[J] = OrderedInt64.ASCENDING
    override val descDataType: DataType[J] = OrderedInt64.DESCENDING
    override def s2j(s: Long): J = s
    override def j2s(j: J): Long = j.intValue()
  }

  implicit object FloatOrderedByteable extends OrderedByteable[Float] {
    type J = jl.Float
    override val ascDataType: DataType[J] = OrderedFloat32.ASCENDING
    override val descDataType: DataType[J] = OrderedFloat32.DESCENDING
    override def s2j(s: Float): J = s
    override def j2s(j: J): Float = j.floatValue()
  }

  implicit object DoubleOrderedByteable extends OrderedByteable[Double] {
    type J = jl.Double
    override val ascDataType: DataType[J] = OrderedFloat64.ASCENDING
    override val descDataType: DataType[J] = OrderedFloat64.DESCENDING
    override def s2j(s: Double): J = s
    override def j2s(j: J): Double = j.doubleValue()
  }

  implicit object BytesOrderedByteable extends OrderedByteable[Array[Byte]] {
    type J = Array[Byte]
    override val ascDataType: DataType[J] = OrderedBlobVar.ASCENDING
    override val descDataType: DataType[J] = OrderedBlobVar.DESCENDING
    override def s2j(s: Array[Byte]): Array[Byte] = s
    override def j2s(j: Array[Byte]): Array[Byte] = j
  }

  implicit object StringOrderedByteable extends OrderedByteable[String] {
    type J = String
    override val ascDataType: DataType[J] = OrderedString.ASCENDING
    override val descDataType: DataType[J] = OrderedString.DESCENDING
    override def s2j(s: String): String = s
    override def j2s(j: String): String = j
  }

  implicit object BigDecimalOrderedByteable extends OrderedByteable[BigDecimal] {
    type J = Number
    override val ascDataType: DataType[J] = OrderedNumeric.ASCENDING
    override val descDataType: DataType[J] = OrderedNumeric.DESCENDING
    override def s2j(s: BigDecimal): Number = s.underlying()
    override def j2s(j: Number): BigDecimal = BigDecimal(j.asInstanceOf[java.math.BigDecimal])
  }

  implicit object UUIDOrderedByteable extends OrderedByteable[UUID] {
    private class UUIDDataType(longDataType: DataType[jl.Long]) extends DataType[UUID] {
      override def encode(dst: PositionedByteRange, value: UUID): Int = {
        longDataType.encode(dst, value.getMostSignificantBits)
        longDataType.encode(dst, value.getLeastSignificantBits)
      }

      override def decode(src: PositionedByteRange): UUID =
        new UUID(longDataType.decode(src), longDataType.decode(src))

      override def skip(src: PositionedByteRange): Int = {
        longDataType.skip(src)
        longDataType.skip(src)
      }

      override def encodedClass(): Class[UUID] = classOf[UUID]

      override def encodedLength(value: UUID): Int =
        longDataType.encodedLength(value.getMostSignificantBits) +
          longDataType.encodedLength(value.getLeastSignificantBits)

      override def isSkippable: Boolean = longDataType.isSkippable

      override def isNullable: Boolean = false

      override def getOrder: Order = Order.ASCENDING

      override def isOrderPreserving: Boolean = longDataType.isOrderPreserving
    }

    type J = UUID
    override implicit def j2s(j: J): UUID = j
    override implicit def s2j(s: UUID): J = s
    override val descDataType: DataType[J] = new UUIDDataType(OrderedInt64.DESCENDING)
    override val ascDataType: DataType[J] = new UUIDDataType(OrderedInt64.ASCENDING)
  }

  implicit def Tuple2OrderedByteable[T1, T2](implicit t1: OrderedByteable[T1], t2: OrderedByteable[T2]): OrderedByteable[(T1, T2)] =
    new OrderedByteable[(T1, T2)] {
      type J = Array[AnyRef]
      override val ascDataType: DataType[J] = new Struct(Array(t1.ascDataType, t2.ascDataType))
      override val descDataType: DataType[J] = new Struct(Array(t1.descDataType, t2.descDataType))
      override def s2j(s: (T1, T2)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2])
    }

  implicit def Tuple3OrderedByteable[T1, T2, T3](implicit t1: OrderedByteable[T1], t2: OrderedByteable[T2], t3: OrderedByteable[T3]): OrderedByteable[(T1, T2, T3)] =
    new OrderedByteable[(T1, T2, T3)] {
      type J = Array[AnyRef]
      override val ascDataType: DataType[J] = new Struct(Array(t1.ascDataType, t2.ascDataType, t3.ascDataType))
      override val descDataType: DataType[J] = new Struct(Array(t1.descDataType, t2.descDataType, t3.descDataType))
      override def s2j(s: (T1, T2, T3)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3])
    }

  implicit def Tuple4OrderedByteable[T1, T2, T3, T4](implicit t1: OrderedByteable[T1], t2: OrderedByteable[T2], t3: OrderedByteable[T3], t4: OrderedByteable[T4]): OrderedByteable[(T1, T2, T3, T4)] =
    new OrderedByteable[(T1, T2, T3, T4)] {
      type J = Array[AnyRef]
      override val ascDataType: DataType[J] = new Struct(Array(t1.ascDataType, t2.ascDataType, t3.ascDataType, t4.ascDataType))
      override val descDataType: DataType[J] = new Struct(Array(t1.descDataType, t2.descDataType, t3.descDataType, t4.descDataType))
      override def s2j(s: (T1, T2, T3, T4)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3, T4) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3], j(3).asInstanceOf[T4])
    }

  implicit def Tuple5OrderedByteable[T1, T2, T3, T4, T5](implicit t1: OrderedByteable[T1], t2: OrderedByteable[T2], t3: OrderedByteable[T3], t4: OrderedByteable[T4], t5: OrderedByteable[T5]): OrderedByteable[(T1, T2, T3, T4, T5)] =
    new OrderedByteable[(T1, T2, T3, T4, T5)] {
      type J = Array[AnyRef]
      override val ascDataType: DataType[J] = new Struct(Array(t1.ascDataType, t2.ascDataType, t3.ascDataType, t4.ascDataType, t5.ascDataType))
      override val descDataType: DataType[J] = new Struct(Array(t1.descDataType, t2.descDataType, t3.descDataType, t4.descDataType, t5.descDataType))
      override def s2j(s: (T1, T2, T3, T4, T5)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3, T4, T5) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3], j(3).asInstanceOf[T4], j(4).asInstanceOf[T5])
    }
}
