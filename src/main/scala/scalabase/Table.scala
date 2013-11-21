package scalabase

import scalabase.structures.{IntervalSet, Interval}

trait Table[+R, +F, +Q, +V] {

  case class Cell(rowkey: R, family: F, qualifier: Q, version: Long, value: V)

  def row(row: R): Table[R, F, Q, V]

  def rows(rows: Interval[R]): Table[R, F, Q, V]

  def rows(rows: IntervalSet[R]): Table[R, F, Q, V]

  def rows: Option[IntervalSet[R]]


  def columnFamily(family: F): Table[R, F, Q, V]

  def columnFamilies(families: Set[F]): Table[R, F, Q, V]

  def columnFamilies: Option[Set[F]]


  def column(family: F, qualifier: Q): Table[R, F, Q, V]

  def columns(family: F, qualifiers: IntervalSet[Q]): Table[R, F, Q, V]

  def columns(columns: Map[F, IntervalSet[Q]]): Table[R, F, Q, V]

  def columns: Option[Map[F, IntervalSet[Q]]]


  def version(version: Long): Table[R, F, Q, V]

  def versions(versions: IntervalSet[Long]): Table[R, F, Q, V]

  def versions: Option[IntervalSet[Long]]

}
