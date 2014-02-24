package carthorse

import java.{lang => jl, util => ju}

import scala.language.implicitConversions

import org.apache.hadoop.hbase.util.{PositionedByteRange, Order, SimplePositionedByteRange}
import org.apache.hadoop.hbase.types._

trait Byteable[T] {
  type J <: AnyRef // Corresponding type to T in java API
  def dataType: DataType[J]
  def structDataType: DataType[J] = dataType // Special encoding to support structs
  implicit def s2j(s: T): J
  implicit def j2s(j: J): T
}

object Byteable {

  def toBytes[T](value: T)(implicit ev: Byteable[T]): Array[Byte] = {
    import ev._
    val bytes = new SimplePositionedByteRange(dataType.encodedLength(value))
    dataType.encode(bytes, value)
    bytes.getBytes
  }

  def fromBytes[T](bytes: Array[Byte])(implicit ev: Byteable[T]): T = {
    import ev._
    dataType.decode(new SimplePositionedByteRange(bytes))
  }

  implicit object ByteByteable extends Byteable[Byte] {
    type J = jl.Byte
    override val dataType: DataType[J] = new RawByte
    override def s2j(s: Byte): J = s
    override def j2s(j: J): Byte = j.byteValue()
  }

  implicit object ShortByteable extends Byteable[Short] {
    type J = jl.Short
    override val dataType: DataType[J] = new RawShort
    override def s2j(s: Short): J = s
    override def j2s(j: J): Short = j.shortValue()
  }

  implicit object IntByteable extends Byteable[Int] {
    type J = jl.Integer
    override val dataType: DataType[J] = new RawInteger
    override def s2j(s: Int): J = s
    override def j2s(j: J): Int = j.intValue()
  }

  implicit object LongByteable extends Byteable[Long] {
    type J = jl.Long
    override val dataType: DataType[J] = new RawLong
    override def s2j(s: Long): J = s
    override def j2s(j: J): Long = j.intValue()
  }

  implicit object FloatByteable extends Byteable[Float] {
    type J = jl.Float
    override val dataType: DataType[J] = new RawFloat
    override def s2j(s: Float): J = s
    override def j2s(j: J): Float = j.floatValue()
  }

  implicit object DoubleByteable extends Byteable[Double] {
    type J = jl.Double
    override val dataType: DataType[J] = new RawDouble
    override def s2j(s: Double): J = s
    override def j2s(j: J): Double = j.doubleValue()
  }

  implicit object BytesByteable extends Byteable[Array[Byte]] {
    type J = Array[Byte]
    override val dataType: DataType[J] = RawBytes.ASCENDING
    override val structDataType: DataType[J] = PrefixedRawBytes
    override def s2j(s: Array[Byte]): Array[Byte] = s
    override def j2s(j: Array[Byte]): Array[Byte] = j
  }

  implicit object StringByteable extends Byteable[String] {
    type J = String
    override val dataType: DataType[J] = RawString.ASCENDING
    override val structDataType: DataType[J] = PrefixedRawString
    override def s2j(s: String): String = s
    override def j2s(j: String): String = j
  }

  implicit object BigDecimalByteable extends Byteable[BigDecimal] {
    type J = Number
    override val dataType: DataType[J] = OrderedNumeric.ASCENDING
    override def s2j(s: BigDecimal): Number = s.underlying()
    override def j2s(j: Number): BigDecimal = BigDecimal(j.asInstanceOf[java.math.BigDecimal])
  }

  implicit def Tuple2Byteable[T1, T2](implicit t1: Byteable[T1], t2: Byteable[T2]): Byteable[(T1, T2)] =
    new Byteable[(T1, T2)] {
      type J = Array[AnyRef]
      override val dataType: DataType[J] = new Struct(Array(t1.structDataType, t2.structDataType))
      override def s2j(s: (T1, T2)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2])
    }

  implicit def Tuple3Byteable[T1, T2, T3](implicit t1: Byteable[T1], t2: Byteable[T2], t3: Byteable[T3]): Byteable[(T1, T2, T3)] =
    new Byteable[(T1, T2, T3)] {
      type J = Array[AnyRef]
      override val dataType: DataType[J] = new Struct(Array(t1.structDataType, t2.structDataType, t3.structDataType))
      override def s2j(s: (T1, T2, T3)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3])
    }

  implicit def Tuple4Byteable[T1, T2, T3, T4](implicit t1: Byteable[T1], t2: Byteable[T2], t3: Byteable[T3], t4: Byteable[T4]): Byteable[(T1, T2, T3, T4)] =
    new Byteable[(T1, T2, T3, T4)] {
      type J = Array[AnyRef]
      override val dataType: DataType[J] = new Struct(Array(t1.structDataType, t2.structDataType, t3.structDataType, t4.structDataType))
      override def s2j(s: (T1, T2, T3, T4)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3, T4) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3], j(3).asInstanceOf[T4])
    }

  implicit def Tuple5Byteable[T1, T2, T3, T4, T5](implicit t1: Byteable[T1], t2: Byteable[T2], t3: Byteable[T3], t4: Byteable[T4], t5: Byteable[T5]): Byteable[(T1, T2, T3, T4, T5)] =
    new Byteable[(T1, T2, T3, T4, T5)] {
      type J = Array[AnyRef]
      override val dataType: DataType[J] = new Struct(Array(t1.structDataType, t2.structDataType, t3.structDataType, t4.structDataType, t5.structDataType))
      override def s2j(s: (T1, T2, T3, T4, T5)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3, T4, T5) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3], j(3).asInstanceOf[T4], j(4).asInstanceOf[T5])
    }

  private object PrefixedRawString extends DataType[String] {
    override def encode(dst: PositionedByteRange, value: String): Int = {
      val bytes = value.getBytes(Charset)
      OrderedNumeric.ASCENDING.encodeLong(dst, bytes.length) + RawBytes.ASCENDING.encode(dst, bytes)
    }

    override def decode(src: PositionedByteRange): String = {
      val length = OrderedNumeric.ASCENDING.decodeLong(src).toInt
      val s = new String(src.getBytes, src.getPosition, length, Charset)
      src.setPosition(src.getPosition + length)
      s
    }

    override def skip(src: PositionedByteRange): Int = {
      val start = src.getPosition
      val length = OrderedNumeric.ASCENDING.decodeLong(src).toInt
      src.setPosition(length + src.getPosition)
      src.getPosition - start
    }

    override def encodedClass(): Class[String] = classOf[String]

    override def encodedLength(value: String): Int = {
      val bytesLength = value.getBytes(Charset).length
      OrderedNumeric.ASCENDING.encodedLength(bytesLength) + bytesLength
    }

    override def isSkippable: Boolean = true

    override def isNullable: Boolean = false

    override def getOrder: Order = Order.ASCENDING

    override def isOrderPreserving: Boolean = false
  }

  private object PrefixedRawBytes extends DataType[Array[Byte]] {
    override def encode(dst: PositionedByteRange, value: Array[Byte]): Int =
      OrderedNumeric.ASCENDING.encodeLong(dst, value.length) + RawBytes.ASCENDING.encode(dst, value)

    override def decode(src: PositionedByteRange): Array[Byte] = {
      val length = OrderedNumeric.ASCENDING.decodeLong(src).toInt
      ju.Arrays.copyOfRange(src.getBytes, src.getPosition, src.getPosition + length)
    }

    override def skip(src: PositionedByteRange): Int = {
      val start = src.getPosition
      val length = OrderedNumeric.ASCENDING.decodeLong(src).toInt
      src.setPosition(length + src.getPosition)
      src.getPosition - start
    }

    override def encodedClass(): Class[Array[Byte]] = classOf[Array[Byte]]

    override def encodedLength(value: Array[Byte]): Int =
      OrderedNumeric.ASCENDING.encodedLength(value.length) + value.length

    override def isSkippable: Boolean = true

    override def isNullable: Boolean = false

    override def getOrder: Order = Order.ASCENDING

    override def isOrderPreserving: Boolean = false
  }
}
