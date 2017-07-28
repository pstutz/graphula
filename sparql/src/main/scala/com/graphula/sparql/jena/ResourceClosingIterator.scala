package com.graphula.sparql.jena

import com.graphula.util.HybridIterator

final class ResourceClosingIterator[G](wrapped: Iterator[G], onClose: => Unit)
    extends HybridIterator[G] {

  private var closed = false
  def close(): Unit = onClose

  @inline override def hasNext: Boolean = {
    wrapped.hasNext || {
      if (!closed) {
        closed = true
        close()
      }
      false
    }
  }

  @inline override def next(): G = wrapped.next()

  override def size(): Int = {
    var i = 0
    while (wrapped.hasNext) {
      wrapped.next
      i += 1
    }
    close()
    i
  }

}
