package com.jettdb
package ingest

import scala.collection.concurrent.TrieMap

class Memtable[T](codec: Codec[T]) {

  private val memtable: TrieMap[String, T] = TrieMap[String, T]()

  // Put
  private def put(key: String, value: T): Unit = {
    memtable.put(key, value)
  }

  // Get
  private def get(key: String): Option[T] = {
    memtable.get(key)
  }

  private def flushToDisk(): Unit = {
    val rows = memtable.toList
    SSTable(rows, codec)
    memtable.clear()
  }
}
