package carthorse

import com.google.common.collect.{Range, RangeSet}

//trait Table[+R <: Ordered[R], +F <: Ordered[F], +Q <: Ordered[Q], +V <: Ordered[V]] {
//
//  case class Cell(rowkey: R, family: F, qualifier: Q, version: Long, value: V)
//
//  def row(row: R): Table[R, F, Q, V]
//
//  def rows(rows: Range[R]): Table[R, F, Q, V]
//
//  def rows(rows: RangeSet[R]): Table[R, F, Q, V]
//
//  def rows: RangeSet[R]
//
//
//  def columnFamily(family: F): Table[R, F, Q, V]
//
//  def columnFamilies(families: Set[F]): Table[R, F, Q, V]
//
//  def columnFamilies: Option[Set[F]]
//
//
//  def column(family: F, qualifier: Q): Table[R, F, Q, V]
//
//  def columns(family: F, qualifiers: RangeSet[Q]): Table[R, F, Q, V]
//
//  def columns(columns: Map[F, RangeSet[Q]]): Table[R, F, Q, V]
//
//  def columns: Option[Map[F, RangeSet[Q]]]
//
//
//  def version(version: Long): Table[R, F, Q, V]
//
//  def versions(versions: RangeSet[Long]): Table[R, F, Q, V]
//
//  def versions: Option[RangeSet[Long]]
//
//}
