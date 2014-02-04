package carthorse

import java.{lang => jl}

import org.apache.hadoop.hbase.util.SimplePositionedByteRange
import org.apache.hadoop.hbase.types._
import com.google.common.primitives.UnsignedBytes

trait OrderedByteable[T] extends Ordered[T] {
  type J <: AnyRef // Corresponding type to T in java API
  def ascDataType: DataType[J]
  def descDataType: DataType[J]
  implicit def s2j(s: T): J
  implicit def j2s(j: J): T
}

object OrderedByteable {

  def toBytesAsc[T <% OrderedByteable[T]](value: T): Array[Byte] = {
    val ascDataType = value.ascDataType
    val j = value.s2j(value)
    val bytes = new SimplePositionedByteRange(ascDataType.encodedLength(j))
    ascDataType.encode(bytes, j)
    bytes.getBytes
  }

  def toBytesDesc[T <% OrderedByteable[T]](value: T): Array[Byte] = {
    val descDataType = value.descDataType
    val j = value.s2j(value)
    val bytes = new SimplePositionedByteRange(descDataType.encodedLength(j))
    descDataType.encode(bytes, j)
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

  implicit def ordered[T : OrderedByteable](t: T): Ordered[T] = new Ordered[T] {
    override def compare(that: T): Int = implicitly[Ordered[T]](t).compare(that)
  }

  implicit class ByteOrderedByteable(self: Byte) extends OrderedByteable[Byte] {
    override type J = jl.Byte
    override def ascDataType: DataType[J] = OrderedInt8.ASCENDING
    override def descDataType: DataType[J] = OrderedInt8.DESCENDING
    override def s2j(s: Byte): J = s
    override def j2s(j: J): Byte = j.byteValue()
    override def compare(other: Byte): Int = Ordering.Int.compare(self, other)
  }

  implicit class ShortOrderedByteable(self: Short) extends OrderedByteable[Short] {
    override type J = jl.Short
    override def ascDataType: DataType[J] = OrderedInt16.ASCENDING
    override def descDataType: DataType[J] = OrderedInt16.DESCENDING
    override def s2j(s: Short): J = s
    override def j2s(j: J): Short = j.shortValue()
    override def compare(other: Short): Int = Ordering.Short.compare(self, other)
  }

  implicit class IntOrderedByteable(self: Int) extends OrderedByteable[Int] {
    override type J = jl.Integer
    override def ascDataType: DataType[J] = OrderedInt32.ASCENDING
    override def descDataType: DataType[J] = OrderedInt32.DESCENDING
    override def s2j(s: Int): J = s
    override def j2s(j: J): Int = j.intValue()
    override def compare(other: Int): Int = Ordering.Int.compare(self, other)
  }

  implicit class LongOrderedByteable(self: Long) extends OrderedByteable[Long] {
    override type J = jl.Long
    override def ascDataType: DataType[J] = OrderedInt64.ASCENDING
    override def descDataType: DataType[J] = OrderedInt64.DESCENDING
    override def s2j(s: Long): J = s
    override def j2s(j: J): Long = j.intValue()
    override def compare(other: Long): Int = Ordering.Long.compare(self, other)
  }

  implicit class FloatOrderedByteable(self: Float) extends OrderedByteable[Float] {
    override type J = jl.Float
    override def ascDataType: DataType[J] = OrderedFloat32.ASCENDING
    override def descDataType: DataType[J] = OrderedFloat32.DESCENDING
    override def s2j(s: Float): J = s
    override def j2s(j: J): Float = j.floatValue()
    override def compare(other: Float): Int = Ordering.Float.compare(self, other)
  }

  implicit class DoubleOrderedByteable(self: Double) extends OrderedByteable[Double] {
    override type J = jl.Double
    override def ascDataType: DataType[J] = OrderedFloat64.ASCENDING
    override def descDataType: DataType[J] = OrderedFloat64.DESCENDING
    override def s2j(s: Double): J = s
    override def j2s(j: J): Double = j.doubleValue()
    override def compare(other: Double): Int = Ordering.Double.compare(self, other)
  }

  implicit class BytesOrderedByteable(self: Array[Byte]) extends OrderedByteable[Array[Byte]] {
    override type J = Array[Byte]
    override def ascDataType: DataType[J] = OrderedBlobVar.ASCENDING
    override def descDataType: DataType[J] = OrderedBlobVar.DESCENDING
    override def s2j(s: Array[Byte]): Array[Byte] = s
    override def j2s(j: Array[Byte]): Array[Byte] = j
    override def compare(other: Array[Byte]): Int =
      UnsignedBytes.lexicographicalComparator().compare(self, other)
  }

  implicit class StringOrderedByteable(self: String) extends OrderedByteable[String] {
    override type J = String
    override def ascDataType: DataType[J] = OrderedString.ASCENDING
    override def descDataType: DataType[J] = OrderedString.DESCENDING
    override def s2j(s: String): String = s
    override def j2s(j: String): String = j
    override def compare(other: String): Int = Ordering.String.compare(self, other)
  }

  implicit class BigDecimalOrderedByteable(self: BigDecimal) extends OrderedByteable[BigDecimal] {
    override type J = Number
    override def ascDataType: DataType[J] = OrderedNumeric.ASCENDING
    override def descDataType: DataType[J] = OrderedNumeric.DESCENDING
    override def s2j(s: BigDecimal): Number = s.underlying()
    override def j2s(j: Number): BigDecimal = BigDecimal(j.asInstanceOf[java.math.BigDecimal])
    override def compare(other: BigDecimal): Int = Ordering.BigDecimal.compare(self, other)
  }

  implicit class Tuple2OrderedByteable[
      T1 : OrderedByteable,
      T2 : OrderedByteable](self: (T1, T2))
  extends OrderedByteable[(T1, T2)] {
    override type J = Array[AnyRef]
    override def ascDataType: DataType[J] = new Struct(Array(
      implicitly[OrderedByteable[T1]].ascDataType,
      implicitly[OrderedByteable[T2]].ascDataType)
    )
    override def descDataType: DataType[J] = new Struct(Array(
      implicitly[OrderedByteable[T1]].descDataType,
      implicitly[OrderedByteable[T2]].descDataType)
    )
    override implicit def s2j(s: (T1, T2)): J = Array(
      s._1.asInstanceOf[AnyRef],
      s._2.asInstanceOf[AnyRef]
    )
    override implicit def j2s(j: J): (T1, T2) = (
      j(0).asInstanceOf[T1],
      j(1).asInstanceOf[T2]
    )
    override def compare(other: (T1, T2)): Int = Ordering.Tuple2[T1, T2].compare(self, other)
  }

  implicit class Tuple3OrderedByteable[
      T1 : OrderedByteable,
      T2 : OrderedByteable,
      T3 : OrderedByteable](self: (T1, T2, T3))
  extends OrderedByteable[(T1, T2, T3)] {
    override type J = Array[AnyRef]
    override def ascDataType: DataType[J] = new Struct(Array(
      implicitly[OrderedByteable[T1]].ascDataType,
      implicitly[OrderedByteable[T2]].ascDataType,
      implicitly[OrderedByteable[T3]].ascDataType)
    )
    override def descDataType: DataType[J] = new Struct(Array(
      implicitly[OrderedByteable[T1]].descDataType,
      implicitly[OrderedByteable[T2]].descDataType,
      implicitly[OrderedByteable[T3]].descDataType)
    )
    override implicit def s2j(s: (T1, T2, T3)): J = Array(
      s._1.asInstanceOf[AnyRef],
      s._2.asInstanceOf[AnyRef],
      s._3.asInstanceOf[AnyRef]
    )
    override implicit def j2s(j: J): (T1, T2, T3) = (
      j(0).asInstanceOf[T1],
      j(1).asInstanceOf[T2],
      j(2).asInstanceOf[T3]
    )
    override def compare(other: (T1, T2, T3)): Int = Ordering.Tuple3[T1, T2, T3].compare(self, other)
  }

  implicit class Tuple4OrderedByteable[
      T1 : OrderedByteable,
      T2 : OrderedByteable,
      T3 : OrderedByteable,
      T4 : OrderedByteable](self: (T1, T2, T3, T4))
  extends OrderedByteable[(T1, T2, T3, T4)] {
    override type J = Array[AnyRef]
    override def ascDataType: DataType[J] = new Struct(Array(
      implicitly[OrderedByteable[T1]].ascDataType,
      implicitly[OrderedByteable[T2]].ascDataType,
      implicitly[OrderedByteable[T3]].ascDataType,
      implicitly[OrderedByteable[T4]].ascDataType)
    )
    override def descDataType: DataType[J] = new Struct(Array(
      implicitly[OrderedByteable[T1]].descDataType,
      implicitly[OrderedByteable[T2]].descDataType,
      implicitly[OrderedByteable[T3]].descDataType,
      implicitly[OrderedByteable[T4]].descDataType)
    )
    override implicit def s2j(s: (T1, T2, T3, T4)): J = Array(
      s._1.asInstanceOf[AnyRef],
      s._2.asInstanceOf[AnyRef],
      s._3.asInstanceOf[AnyRef],
      s._4.asInstanceOf[AnyRef]
    )
    override implicit def j2s(j: J): (T1, T2, T3, T4) = (
      j(0).asInstanceOf[T1],
      j(1).asInstanceOf[T2],
      j(2).asInstanceOf[T3],
      j(3).asInstanceOf[T4]
    )
    override def compare(other: (T1, T2, T3, T4)): Int = Ordering.Tuple4[T1, T2, T3, T4].compare(self, other)
  }

  implicit class Tuple5OrderedByteable[
      T1 : OrderedByteable,
      T2 : OrderedByteable,
      T3 : OrderedByteable,
      T4 : OrderedByteable,
      T5 : OrderedByteable](self: (T1, T2, T3, T4, T5))
  extends OrderedByteable[(T1, T2, T3, T4, T5)] {
    override type J = Array[AnyRef]
    override def ascDataType: DataType[J] = new Struct(Array(
      implicitly[OrderedByteable[T1]].ascDataType,
      implicitly[OrderedByteable[T2]].ascDataType,
      implicitly[OrderedByteable[T3]].ascDataType,
      implicitly[OrderedByteable[T4]].ascDataType,
      implicitly[OrderedByteable[T5]].ascDataType)
    )
    override def descDataType: DataType[J] = new Struct(Array(
      implicitly[OrderedByteable[T1]].descDataType,
      implicitly[OrderedByteable[T2]].descDataType,
      implicitly[OrderedByteable[T3]].descDataType,
      implicitly[OrderedByteable[T4]].descDataType,
      implicitly[OrderedByteable[T5]].descDataType)
    )
    override implicit def s2j(s: (T1, T2, T3, T4, T5)): J = Array(
      s._1.asInstanceOf[AnyRef],
      s._2.asInstanceOf[AnyRef],
      s._3.asInstanceOf[AnyRef],
      s._4.asInstanceOf[AnyRef],
      s._5.asInstanceOf[AnyRef]
    )
    override implicit def j2s(j: J): (T1, T2, T3, T4, T5) = (
      j(0).asInstanceOf[T1],
      j(1).asInstanceOf[T2],
      j(2).asInstanceOf[T3],
      j(3).asInstanceOf[T4],
      j(4).asInstanceOf[T5]
    )
    override def compare(other: (T1, T2, T3, T4, T5)): Int = Ordering.Tuple5[T1, T2, T3, T4, T5].compare(self, other)
  }
}
