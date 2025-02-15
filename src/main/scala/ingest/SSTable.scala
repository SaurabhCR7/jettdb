package com.jettdb
package ingest

class SSTable {

}

object SSTable {

  def apply[T](rows: List[(String, T)], codec: Codec[T]): SSTable = {
    implicit val ordering: Ordering[T] = Ordering.by(_.toString)
    val sortedRows = rows.sortBy(_._1)
    sortedRows.foreach(row => {
      val (key, value) = row
      println(s"Writing key: $key, value: $value")
      val valueBytes = codec.encode(value)
    })
    new SSTable()
  }
}
