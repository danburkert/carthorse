package scalabase

trait Table[R, F, Q, V] {
  case class Cell(rowkey: R, family: F, qualifier: Q, timestamp: Long, value: V)

}
