package carthorse

import java.{lang => jl, util => ju}

import scala.reflect.runtime.universe._
import scala.language.implicitConversions

import org.apache.hadoop.hbase.util.{PositionedByteRange, Order, SimplePositionedByteRange}
import org.apache.hadoop.hbase.types._
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.reflect.ClassTag


trait Byteable[T] {
  def toBytes(value: T): Array[Byte]
  def fromBytes(bytes: Array[Byte]): T
}

trait DataTypeByteable[T] extends Byteable[T] {
  type J <: AnyRef // Corresponding type to T in java API
  def dataType: DataType[J]
  def structDataType: DataType[J] = dataType // Special encoding to support structs
  implicit def s2j(s: T): J
  implicit def j2s(j: J): T

  override def fromBytes(bytes: Array[Byte]): T =
    dataType.decode(new SimplePositionedByteRange(bytes))

  override def toBytes(value: T): Array[Byte] = {
    val bytes = new SimplePositionedByteRange(dataType.encodedLength(value))
    dataType.encode(bytes, value)
    bytes.getBytes
  }
}

object Byteable {

  def toBytes[T](value: T)(implicit ev: Byteable[T]): Array[Byte] = ev.toBytes(value)

  def fromBytes[T](bytes: Array[Byte])(implicit ev: Byteable[T]): T = ev.fromBytes(bytes)

  implicit object ByteByteable extends DataTypeByteable[Byte] {
    type J = jl.Byte
    override val dataType: DataType[J] = new RawByte
    override def s2j(s: Byte): J = s
    override def j2s(j: J): Byte = j.byteValue()
  }

  implicit object ShortByteable extends DataTypeByteable[Short] {
    type J = jl.Short
    override val dataType: DataType[J] = new RawShort
    override def s2j(s: Short): J = s
    override def j2s(j: J): Short = j.shortValue()
  }

  implicit object IntByteable extends DataTypeByteable[Int] {
    type J = jl.Integer
    override val dataType: DataType[J] = new RawInteger
    override def s2j(s: Int): J = s
    override def j2s(j: J): Int = j.intValue()
  }

  implicit object LongByteable extends DataTypeByteable[Long] {
    type J = jl.Long
    override val dataType: DataType[J] = new RawLong
    override def s2j(s: Long): J = s
    override def j2s(j: J): Long = j.intValue()
  }

  implicit object FloatByteable extends DataTypeByteable[Float] {
    type J = jl.Float
    override val dataType: DataType[J] = new RawFloat
    override def s2j(s: Float): J = s
    override def j2s(j: J): Float = j.floatValue()
  }

  implicit object DoubleByteable extends DataTypeByteable[Double] {
    type J = jl.Double
    override val dataType: DataType[J] = new RawDouble
    override def s2j(s: Double): J = s
    override def j2s(j: J): Double = j.doubleValue()
  }

  implicit object BytesByteable extends DataTypeByteable[Array[Byte]] {
    type J = Array[Byte]
    override val dataType: DataType[J] = RawBytes.ASCENDING
    override val structDataType: DataType[J] = PrefixedRawBytes
    override def s2j(s: Array[Byte]): Array[Byte] = s
    override def j2s(j: Array[Byte]): Array[Byte] = j
  }

  implicit object StringByteable extends DataTypeByteable[String] {
    type J = String
    override val dataType: DataType[J] = RawString.ASCENDING
    override val structDataType: DataType[J] = PrefixedRawString
    override def s2j(s: String): String = s
    override def j2s(j: String): String = j
  }

  implicit object BigDecimalByteable extends DataTypeByteable[BigDecimal] {
    type J = Number
    override val dataType: DataType[J] = OrderedNumeric.ASCENDING
    override def s2j(s: BigDecimal): Number = s.underlying()
    override def j2s(j: Number): BigDecimal = BigDecimal(j.asInstanceOf[java.math.BigDecimal])
  }

  //implicit def SpecificAvroByteable[T <: SpecificRecord](implicit ev: Manifest[T]): Byteable[T] = new Byteable[T] {
    //override def fromBytes(bytes: Array[Byte]): T = {
      //val reader = new SpecificDatumReader[T](ev.runtimeClass.asSubclass[T])
      //val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
      //reader.read(_: T, decoder)
    //}

    //override def toBytes(value: T): Array[Byte] = {
      //val writer = new SpecificDatumWriter[T](classOf[T])
      //val baos = new ByteArrayOutputStream()
      //val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
      //writer.write(value, encoder)
      //baos.toByteArray
    //}
  //}

  implicit def Tuple2Byteable[T1, T2](implicit t1: DataTypeByteable[T1], t2: DataTypeByteable[T2]): Byteable[(T1, T2)] =
    new DataTypeByteable[(T1, T2)] {
      type J = Array[AnyRef]
      override val dataType: DataType[J] = new Struct(Array(t1.structDataType, t2.structDataType))
      override def s2j(s: (T1, T2)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2])
    }

  implicit def Tuple3Byteable[T1, T2, T3](implicit t1: DataTypeByteable[T1], t2: DataTypeByteable[T2], t3: DataTypeByteable[T3]): Byteable[(T1, T2, T3)] =
    new DataTypeByteable[(T1, T2, T3)] {
      type J = Array[AnyRef]
      override val dataType: DataType[J] = new Struct(Array(t1.structDataType, t2.structDataType, t3.structDataType))
      override def s2j(s: (T1, T2, T3)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3])
    }

  implicit def Tuple4Byteable[T1, T2, T3, T4](implicit t1: DataTypeByteable[T1], t2: DataTypeByteable[T2], t3: DataTypeByteable[T3], t4: DataTypeByteable[T4]): Byteable[(T1, T2, T3, T4)] =
    new DataTypeByteable[(T1, T2, T3, T4)] {
      type J = Array[AnyRef]
      override val dataType: DataType[J] = new Struct(Array(t1.structDataType, t2.structDataType, t3.structDataType, t4.structDataType))
      override def s2j(s: (T1, T2, T3, T4)): J = Array(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef])
      override def j2s(j: J): (T1, T2, T3, T4) = (j(0).asInstanceOf[T1], j(1).asInstanceOf[T2], j(2).asInstanceOf[T3], j(3).asInstanceOf[T4])
    }

  implicit def Tuple5Byteable[T1, T2, T3, T4, T5](implicit t1: DataTypeByteable[T1], t2: DataTypeByteable[T2], t3: DataTypeByteable[T3], t4: DataTypeByteable[T4], t5: DataTypeByteable[T5]): Byteable[(T1, T2, T3, T4, T5)] =
    new DataTypeByteable[(T1, T2, T3, T4, T5)] {
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
