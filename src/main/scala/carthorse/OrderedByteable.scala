package carthorse

import java.{lang => jl}

import org.apache.hadoop.hbase.util.SimplePositionedByteRange
import org.apache.hadoop.hbase.types._

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
    def ascDataType: DataType[J] = OrderedInt8.ASCENDING
    def descDataType: DataType[J] = OrderedInt8.DESCENDING
    def s2j(s: Byte): J = s
    def j2s(j: J): Byte = j.byteValue()
  }

  implicit object ShortOrderedByteable extends OrderedByteable[Short] {
    type J = jl.Short
    def ascDataType: DataType[J] = OrderedInt16.ASCENDING
    def descDataType: DataType[J] = OrderedInt16.DESCENDING
    def s2j(s: Short): J = s
    def j2s(j: J): Short = j.shortValue()
  }

  implicit object IntOrderedByteable extends OrderedByteable[Int] {
    type J = jl.Integer
    def ascDataType: DataType[J] = OrderedInt32.ASCENDING
    def descDataType: DataType[J] = OrderedInt32.DESCENDING
    def s2j(s: Int): J = s
    def j2s(j: J): Int = j.intValue()
  }

  implicit object LongOrderedByteable extends OrderedByteable[Long] {
    type J = jl.Long
    def ascDataType: DataType[J] = OrderedInt64.ASCENDING
    def descDataType: DataType[J] = OrderedInt64.DESCENDING
    def s2j(s: Long): J = s
    def j2s(j: J): Long = j.intValue()
  }

  implicit object FloatOrderedByteable extends OrderedByteable[Float] {
    type J = jl.Float
    def ascDataType: DataType[J] = OrderedFloat32.ASCENDING
    def descDataType: DataType[J] = OrderedFloat32.DESCENDING
    def s2j(s: Float): J = s
    def j2s(j: J): Float = j.floatValue()
  }

  implicit object DoubleOrderedByteable extends OrderedByteable[Double] {
    type J = jl.Double
    def ascDataType: DataType[J] = OrderedFloat64.ASCENDING
    def descDataType: DataType[J] = OrderedFloat64.DESCENDING
    def s2j(s: Double): J = s
    def j2s(j: J): Double = j.doubleValue()
  }

  implicit object BytesOrderedByteable extends OrderedByteable[Array[Byte]] {
    type J = Array[Byte]
    def ascDataType: DataType[J] = OrderedBlobVar.ASCENDING
    def descDataType: DataType[J] = OrderedBlobVar.DESCENDING
    def s2j(s: Array[Byte]): Array[Byte] = s
    def j2s(j: Array[Byte]): Array[Byte] = j
  }

  implicit object StringOrderedByteable extends OrderedByteable[String] {
    type J = String
    def ascDataType: DataType[J] = OrderedString.ASCENDING
    def descDataType: DataType[J] = OrderedString.DESCENDING
    def s2j(s: String): String = s
    def j2s(j: String): String = j
  }

  implicit object BigDecimalOrderedByteable extends OrderedByteable[BigDecimal] {
    type J = Number
    def ascDataType: DataType[J] = OrderedNumeric.ASCENDING
    def descDataType: DataType[J] = OrderedNumeric.DESCENDING
    def s2j(s: BigDecimal): Number = s.underlying()
    def j2s(j: Number): BigDecimal = BigDecimal(j.asInstanceOf[java.math.BigDecimal])
  }

  implicit def Tuple2OrderedByteable[
  T1 : OrderedByteable,
  T2 : OrderedByteable]:
  OrderedByteable[(T1, T2)] =
    new OrderedByteable[(T1, T2)] {
      type J = Array[AnyRef]
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
    }

  implicit def Tuple3OrderedByteable[
      T1 : OrderedByteable,
      T2 : OrderedByteable,
      T3 : OrderedByteable]:
        OrderedByteable[(T1, T2, T3)] =
    new OrderedByteable[(T1, T2, T3)] {
      type J = Array[AnyRef]
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
    }

  implicit def Tuple4OrderedByteable[
      T1 : OrderedByteable,
      T2 : OrderedByteable,
      T3 : OrderedByteable,
      T4 : OrderedByteable]:
        OrderedByteable[(T1, T2, T3, T4)] =
    new OrderedByteable[(T1, T2, T3, T4)] {
      type J = Array[AnyRef]
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
    }

  implicit def Tuple5OrderedByteable[
      T1 : OrderedByteable,
      T2 : OrderedByteable,
      T3 : OrderedByteable,
      T4 : OrderedByteable,
      T5 : OrderedByteable]:
        OrderedByteable[(T1, T2, T3, T4, T5)] =
    new OrderedByteable[(T1, T2, T3, T4, T5)] {
      type J = Array[AnyRef]
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
    }
}
