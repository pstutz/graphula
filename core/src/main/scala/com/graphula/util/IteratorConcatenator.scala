package com.graphula.util

import scala.annotation.tailrec

/**
 * Contract: `next` can only be called if `hasNext` returned true before calling it.
 */
final class IteratorOfIteratorsConcatenator[G](
  is: HybridIterator[HybridIterator[G]]
)
    extends HybridIterator[G] {

  private var remainingIterators = is
  private var currentIterator: HybridIterator[G] = _

  @tailrec
  @inline def hasNext(): Boolean = {
    if (currentIterator != null && currentIterator.hasNext) {
      true
    } else if (remainingIterators.hasNext) {
      currentIterator = remainingIterators.next()
      hasNext()
    } else {
      false
    }
  }

  @inline def next(): G = {
    currentIterator.next
  }

}

/**
 * Contract: `next` can only be called if `hasNext` returned true before calling it.
 */
final class LazyArrayOfIteratorsConcatenator[G](
  is: Array[() => HybridIterator[G]]
)
    extends HybridIterator[G] {

  private var i = 0
  private var currentIterator: HybridIterator[G] = _

  @tailrec
  @inline def hasNext(): Boolean = {
    if (currentIterator != null && currentIterator.hasNext) {
      true
    } else if (i < is.length) {
      currentIterator = is(i)()
      i += 1
      hasNext()
    } else {
      false
    }
  }

  @inline def next(): G = {
    currentIterator.next()
  }

}

/**
 * Contract: `next` can only be called if `hasNext` returned true before calling it.
 */
final class ArrayOfIteratorsConcatenator[G](is: Array[HybridIterator[G]])
    extends HybridIterator[G] {

  private var i = 0
  private var currentIterator: HybridIterator[G] = _

  @tailrec
  @inline def hasNext(): Boolean = {
    if (currentIterator != null && currentIterator.hasNext) {
      true
    } else if (i < is.length) {
      currentIterator = is(i)
      i += 1
      hasNext()
    } else {
      false
    }
  }

  @inline def next(): G = {
    currentIterator.next()
  }

}
