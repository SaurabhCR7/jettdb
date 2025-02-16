package com.jettdb
package ingest

import scala.collection.concurrent.TrieMap

class Memtable[T](codec: Codec[T]) {

  private val memtable: TrieMap[String, T] = TrieMap[String, T]()

  private def put(key: String, value: T): Unit = {
    memtable.put(key, value)
  }

  private def get(key: String): Option[T] = {
    memtable.get(key)
  }

  private def flushToDisk(): Unit = {
    val rows = memtable.toList
    val sstable = new SSTable[T](codec)
    sstable.create(rows)
    memtable.clear()
  }
}
