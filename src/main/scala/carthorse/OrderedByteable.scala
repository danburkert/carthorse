package carthorse

import java.{lang => jl, util}

import org.apache.hadoop.hbase.util.SimplePositionedByteRange
import org.apache.hadoop.hbase.types._
import java.util.Objects

trait OrderedByteable[T] {
  type J <: AnyRef // Corresponding type to T in java API
  def ascDataType: DataType[J]
  def descDataType: DataType[J]
  implicit def s2j(s: T): J
  implicit def j2s(j: J): T
  def equals(left: T, right: Any): Boolean
  def hashCode(value: T): Int
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

  def equals[T](left: T, right: T)(implicit ev: OrderedByteable[T]): Boolean = {
    ev.equals(left, right)
  }

  implicit object ByteOrderedByteable extends OrderedByteable[Byte] {
    type J = jl.Byte
    override val ascDataType: DataType[J] = OrderedInt8.ASCENDING
    override val descDataType: DataType[J] = OrderedInt8.DESCENDING
    override def s2j(s: Byte): J = s
    override def j2s(j: J): Byte = j.byteValue()
    override def equals(left: Byte, right: Any): Boolean = left == right
    override def hashCode(value: Byte): Int = value.hashCode()
  }

  implicit object ShortOrderedByteable extends OrderedByteable[Short] {
    type J = jl.Short
    override val ascDataType: DataType[J] = OrderedInt16.ASCENDING
    override val descDataType: DataType[J] = OrderedInt16.DESCENDING
    override def s2j(s: Short): J = s
    override def j2s(j: J): Short = j.shortValue()
    override def equals(left: Short, right: Any): Boolean = left == right
    override def hashCode(value: Short): Int = value.hashCode()
  }

  implicit object IntOrderedByteable extends OrderedByteable[Int] {
    type J = jl.Integer
    override val ascDataType: DataType[J] = OrderedInt32.ASCENDING
    override val descDataType: DataType[J] = OrderedInt32.DESCENDING
    override def s2j(s: Int): J = s
    override def j2s(j: J): Int = j.intValue()
    override def equals(left: Int, right: Any): Boolean = left == right
    override def hashCode(value: Int): Int = value.hashCode()
  }

  implicit object LongOrderedByteable extends OrderedByteable[Long] {
    type J = jl.Long
    override val ascDataType: DataType[J] = OrderedInt64.ASCENDING
    override val descDataType: DataType[J] = OrderedInt64.DESCENDING
    override def s2j(s: Long): J = s
    override def j2s(j: J): Long = j.intValue()
    override def equals(left: Long, right: Any): Boolean = left == right
    override def hashCode(value: Long): Int = value.hashCode()
  }

  implicit object FloatOrderedByteable extends OrderedByteable[Float] {
    type J = jl.Float
    override val ascDataType: DataType[J] = OrderedFloat32.ASCENDING
    override val descDataType: DataType[J] = OrderedFloat32.DESCENDING
    override def s2j(s: Float): J = s
    override def j2s(j: J): Float = j.floatValue()
    override def equals(left: Float, right: Any): Boolean = left == right
    override def hashCode(value: Float): Int = value.hashCode()
  }

  implicit object DoubleOrderedByteable extends OrderedByteable[Double] {
    type J = jl.Double
    override val ascDataType: DataType[J] = OrderedFloat64.ASCENDING
    override val descDataType: DataType[J] = OrderedFloat64.DESCENDING
    override def s2j(s: Double): J = s
    override def j2s(j: J): Double = j.doubleValue()
    override def equals(left: Double, right: Any): Boolean = left == right
    override def hashCode(value: Double): Int = value.hashCode()
  }

  implicit object BytesOrderedByteable extends OrderedByteable[Array[Byte]] {
    type J = Array[Byte]
    override val ascDataType: DataType[J] = OrderedBlobVar.ASCENDING
    override val descDataType: DataType[J] = OrderedBlobVar.DESCENDING
    override def s2j(s: Array[Byte]): Array[Byte] = s
    override def j2s(j: Array[Byte]): Array[Byte] = j
    override def equals(left: Array[Byte], right: Any): Boolean = right match {
      case r: Array[Byte] => util.Arrays.equals(left, r)
      case _ => false
    }
    override def hashCode(value: Array[Byte]): Int = util.Arrays.hashCode(value)
  }

  implicit object StringOrderedByteable extends OrderedByteable[String] {
    type J = String
    override val ascDataType: DataType[J] = OrderedString.ASCENDING
    override val descDataType: DataType[J] = OrderedString.DESCENDING
    override def s2j(s: String): String = s
    override def j2s(j: String): String = j
    override def equals(left: String, right: Any): Boolean = left == right
    override def hashCode(value: String): Int = value.hashCode
  }

  implicit object BigDecimalOrderedByteable extends OrderedByteable[BigDecimal] {
    type J = Number
    override val ascDataType: DataType[J] = OrderedNumeric.ASCENDING
    override val descDataType: DataType[J] = OrderedNumeric.DESCENDING
    override def s2j(s: BigDecimal): Number = s.underlying()
    override def j2s(j: Number): BigDecimal = BigDecimal(j.asInstanceOf[java.math.BigDecimal])
    override def equals(left: BigDecimal, right: Any): Boolean = left == right
    override def hashCode(value: BigDecimal): Int = value.hashCode
  }

  implicit def Tuple2OrderedByteable[T1, T2](implicit t1: OrderedByteable[T1], t2: OrderedByteable[T2]): OrderedByteable[(T1, T2)] =
    new OrderedByteable[(T1, T2)] {
      type J = Array[AnyRef]
      override val ascDataType: DataType[J] = new Struct(Array(t1.ascDataType, t2.ascDataType))
      override val descDataType: DataType[J] = new Struct(Array(t1.descDataType, t2.descDataType))
      override def s2j(s: (T1, T2)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2])
      override def equals(left: (T1, T2), right: Any): Boolean = right match {
        case (r1, r2) => t1.equals(left._1, r1) && t2.equals(left._2, r2)
        case _ => false
      }
      override def hashCode(value: (T1, T2)): Int =
        41 * (
          41 + t1.hashCode(value._1)
        ) + t2.hashCode(value._2)
    }

  implicit def Tuple3OrderedByteable[T1, T2, T3](implicit t1: OrderedByteable[T1], t2: OrderedByteable[T2], t3: OrderedByteable[T3]): OrderedByteable[(T1, T2, T3)] =
    new OrderedByteable[(T1, T2, T3)] {
      type J = Array[AnyRef]
      override val ascDataType: DataType[J] = new Struct(Array(t1.ascDataType, t2.ascDataType, t3.ascDataType))
      override val descDataType: DataType[J] = new Struct(Array(t1.descDataType, t2.descDataType, t3.descDataType))
      override def s2j(s: (T1, T2, T3)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3])
      override def equals(left: (T1, T2, T3), right: Any): Boolean = right match {
        case (r1, r2, r3) => t1.equals(left._1, r1) && t2.equals(left._2, r2) && t3.equals(left._3, r3)
        case _ => false
      }
      override def hashCode(value: (T1, T2, T3)): Int =
        41 * (
          41 * (
            41 + t1.hashCode(value._1)
          ) + t2.hashCode(value._2)
        ) + t3.hashCode(value._3)
    }

  implicit def Tuple4OrderedByteable[T1, T2, T3, T4](implicit t1: OrderedByteable[T1], t2: OrderedByteable[T2], t3: OrderedByteable[T3], t4: OrderedByteable[T4]): OrderedByteable[(T1, T2, T3, T4)] =
    new OrderedByteable[(T1, T2, T3, T4)] {
      type J = Array[AnyRef]
      override val ascDataType: DataType[J] = new Struct(Array(t1.ascDataType, t2.ascDataType, t3.ascDataType, t4.ascDataType))
      override val descDataType: DataType[J] = new Struct(Array(t1.descDataType, t2.descDataType, t3.descDataType, t4.descDataType))
      override def s2j(s: (T1, T2, T3, T4)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3, T4) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3], j(3).asInstanceOf[T4])
      override def equals(left: (T1, T2, T3, T4), right: Any): Boolean = right match {
        case (r1, r2, r3, r4) => t1.equals(left._1, r1) && t2.equals(left._2, r2) && t3.equals(left._3, r3) && t4.equals(left._4, r4)
        case _ => false
      }
      override def hashCode(value: (T1, T2, T3, T4)): Int =
        41 * (
          41 * (
            41 * (
              41 + t1.hashCode(value._1)
            ) + t2.hashCode(value._2)
          ) + t3.hashCode(value._3)
        ) + t4.hashCode(value._4)
    }

  implicit def Tuple5OrderedByteable[T1, T2, T3, T4, T5](implicit t1: OrderedByteable[T1], t2: OrderedByteable[T2], t3: OrderedByteable[T3], t4: OrderedByteable[T4], t5: OrderedByteable[T5]): OrderedByteable[(T1, T2, T3, T4, T5)] =
    new OrderedByteable[(T1, T2, T3, T4, T5)] {
      type J = Array[AnyRef]
      override val ascDataType: DataType[J] = new Struct(Array(t1.ascDataType, t2.ascDataType, t3.ascDataType, t4.ascDataType, t5.ascDataType))
      override val descDataType: DataType[J] = new Struct(Array(t1.descDataType, t2.descDataType, t3.descDataType, t4.descDataType, t5.descDataType))
      override def s2j(s: (T1, T2, T3, T4, T5)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3, T4, T5) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3], j(3).asInstanceOf[T4], j(4).asInstanceOf[T5])
      override def equals(left: (T1, T2, T3, T4, T5), right: Any): Boolean = right match {
        case (r1, r2, r3, r4, r5) => t1.equals(left._1, r1) && t2.equals(left._2, r2) && t3.equals(left._3, r3) && t4.equals(left._4, r4) && t5.equals(left._5, r5)
        case _ => false
      }
      override def hashCode(value: (T1, T2, T3, T4, T5)): Int =
        41 * (
          41 * (
            41 * (
              41 * (
                41 + t1.hashCode(value._1)
              ) + t2.hashCode(value._2)
            ) + t3.hashCode(value._3)
          ) + t4.hashCode(value._4)
        ) + t5.hashCode(value._5)
    }
}
