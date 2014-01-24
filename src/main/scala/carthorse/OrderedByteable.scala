package carthorse

import java.{lang => jl}

import org.apache.hadoop.hbase.util.SimplePositionedByteRange
import org.apache.hadoop.hbase.types._

trait OrderedByteable[T] {
  def toBytesAsc: Array[Byte]

  def fromBytesAsc(bytes: Array[Byte]): T
}

trait DT[S] {
  type J <: AnyRef
  def dataType: DataType[J]
  def value: J
  def encode: Array[Byte] = {
    val dt = dataType
    val v = value
    val byteRange = new SimplePositionedByteRange(dt.encodedLength(v))
    dt.encode(byteRange, v)
    byteRange.getBytes
  }
  def decode(bytes: Array[Byte]): J = {
    dataType.decode(new SimplePositionedByteRange(bytes))
  }
}

object DT {

  implicit class ByteDT(byte: Byte) extends DT[Byte] {
    type J = jl.Byte
    def value: jl.Byte = byte
    def dataType: DataType[J] = OrderedInt8.ASCENDING
  }

  implicit class IntDT(int: Int) extends DT[Int] {
    type J = jl.Integer
    def value: jl.Integer = new jl.Integer(int)
    def dataType: DataType[J] = OrderedInt32.ASCENDING
  }

  implicit class Tuple2DT[T1 <% DT[T1], T2 <% DT[T2]](t: (T1, T2)) extends DT[(T1, T2)] {
    type J = Array[AnyRef]
    def value: J = Array(t._1.value, t._2.value)
    def dataTypes: Array[DataType[_]] = Array(t._1.dataType, t._2.dataType)
    def dataType: Struct = new Struct(dataTypes)
  }

//  implicit class Tuple2DataType[T1 : DT, T2 : DT](foo: Tuple2[T1, T2]) extends carthorse.DT[Tuple2[T1, T2]] {
//    def dataType: DataType[_] =
//      new StructBuilder()
//        .add(implicitly[DT[T1]].dataType)
//        .add(implicitly[DT[T2]].dataType)
//        .toStruct
//  }


}
//
//object OrderedByteable {
//
//  def t[A : TypeTag, B : TypeTag](a: A) = typeOf[A] match {
//    case t if t =:= typeOf[Byte] => "bytes"
//    case t if t <:< typeOf[Int] => "int"
//    case t if t =:= typeOf[Tuple1[B]] => "tuple"
//  }
//
////  object ascDataType extends Poly1 {
////    implicit def caseInt = at[Int](_ => OrderedInt32.ASCENDING: DataType[java.lang.Integer])
////    implicit def caseLong = at[Long](_ => OrderedInt64.ASCENDING: DataType[java.lang.Long])
////    implicit def caseTuple[T, U]
////      (implicit st: Case.Aux[T, DataType[T]], su: Case.Aux[U, DataType[U]]) =
////        at[(T, U)] { tuple =>
////          new StructBuilder()
////            .add(ascDataType(tuple._1))
////            .add(ascDataType(tuple._2))
////            .toStruct : DataType[Array[AnyRef]]
////          }
////  }
//
//
//
//  object list extends Poly1 {
//    implicit def caseInt = at[Int](x => List[Int](x))
//    implicit def caseString = at[String](x => List(x))
////    implicit def caseTuple[T, U](implicit st : Case.Aux[T, List[T]], su : Case.Aux[U, List[U]]) =
////      at[(T, U)](t => list(t._1) ++ list(t._2))
//  }
//
//
//
//  object size extends Poly1 {
//    implicit def caseInt = at[Int](x => 1)
//    implicit def caseString = at[String](_.length)
//    implicit def caseFoo = at[scala.Symbol]( x => x )
//    implicit def caseTuple[T, U](implicit st : Case.Aux[T, Int], su : Case.Aux[U, Int]) =
//      at[(T, U)](t => size(t._1)+size(t._2))
//  }
//
//  implicit class OrderedByteableByte(value: Byte) extends OrderedByteable[Byte] {
//    val size = 2
//    def getPositionedByteRange: PositionedByteRange = new SimplePositionedByteRange(size)
//
//    def toBytesAsc: Array[Byte] = {
//      val byteRange = getPositionedByteRange
//      OrderedInt8.ASCENDING.encode(byteRange, value)
//      byteRange.getBytes
//    }
//
//    def fromBytesAsc(bytes: Array[Byte]): Byte =
//      OrderedInt8.ASCENDING.decode(new SimplePositionedByteRange(bytes))
//  }
//
//  implicit class OrderedByteableShort(value: Short) extends OrderedByteable[Short] {
//    val size = 3
//    def getPositionedByteRange: PositionedByteRange = new SimplePositionedByteRange(size)
//
//    def toBytesAsc: Array[Byte] = {
//      val byteRange = getPositionedByteRange
//      OrderedInt16.ASCENDING.encode(byteRange, value)
//      byteRange.getBytes
//    }
//
//    def fromBytesAsc(bytes: Array[Byte]): Short =
//      OrderedInt16.ASCENDING.decode(new SimplePositionedByteRange(bytes))
//  }
//
//}
//
//object Foo {
//  import carthorse.OrderedByteable._
//  12.toByte.toBytesAsc
//}
