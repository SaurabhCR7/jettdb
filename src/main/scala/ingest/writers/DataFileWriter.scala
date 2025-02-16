package com.jettdb
package ingest.writers

import ingest.writers.DataFileWriter.EXTENSION

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer

/**
 * Writes data to a file.
 * First 4 bytes denotes the size (n) of key bytes.
 * Next n bytes denotes the key.
 * Next 4 bytes denote the size (m) of value bytes.
 * Next m bytes denote the value.
 */

class DataFileWriter(filename: String) {

  private val file = new File(s"$filename.$EXTENSION")
  private val fos = new FileOutputStream(file, true)

  def write(keyBytes: Array[Byte], valueBytes: Array[Byte]): Unit = {
    val keySize = keyBytes.length
    val valueSize = valueBytes.length

    val buffer = ByteBuffer.allocate(DataFileWriter.KEY_SIZE_BYTES + keySize + DataFileWriter.VALUE_SIZE_BYTES + valueSize)
    buffer.putInt(keySize)
    buffer.put(keyBytes)
    buffer.putInt(valueSize)
    buffer.put(valueBytes)

    fos.write(buffer.array())
  }

  def close(): Unit = {
    fos.close()
  }
}

private object DataFileWriter {
  private val KEY_SIZE_BYTES = 4
  private val VALUE_SIZE_BYTES = 4
  private val EXTENSION = "dat"
}
