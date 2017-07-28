package com.graphula.util

import java.nio.ByteBuffer

final class ThreadLocalSizedBuffer(initialSize: Int)
    extends ThreadLocal[ByteBuffer] {

  def getSize(size: Int): ByteBuffer = {
    val buffer = super.get
    if (buffer.capacity >= size) {
      buffer.rewind
      buffer.limit(size)
      buffer
    } else {
      val newBuffer = ByteBuffer.allocateDirect(size)
      set(newBuffer)
      newBuffer
    }
  }

  override protected def initialValue(): ByteBuffer = {
    ByteBuffer.allocateDirect(initialSize)
  }

}
