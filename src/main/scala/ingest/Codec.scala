package com.jettdb
package ingest

trait Codec[T] {
  
  def encode(obj: T): Array[Byte]
  
  def decode(bytes: Array[Byte]): T
}
