package com.jettdb
package ingest

import ingest.writers.DataFileWriter

class SSTable[T](codec: Codec[T]) {
  
  implicit private val ordering: Ordering[T] = Ordering.by(_.toString)

  def create(rows: List[(String, T)]): Unit = {
    println(s"Writing ${rows.length} rows to disk")
    val sortedRows = rows.sortBy(_._1)
    val creationTimestamp = System.currentTimeMillis()
    val filename = s"jettdb-$creationTimestamp"
    writeToDataFile(sortedRows, filename)
  }
  
  private def writeToDataFile(sortedRows: List[(String, T)], filename: String): Unit = {
    val dataFileWriter = new DataFileWriter(filename)
    sortedRows.foreach(row => {
      val (key, value) = row
      val keyBytes = key.getBytes
      val valueBytes = codec.encode(value)
      dataFileWriter.write(keyBytes, valueBytes)
    })

    dataFileWriter.close()
  }
}
