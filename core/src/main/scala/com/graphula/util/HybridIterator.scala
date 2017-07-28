package com.graphula.util

import java.util.{ Iterator => JavaIterator }

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.reflect.ClassTag

final object HybridIterator {

  class EmptyIterator extends HybridIterator[Nothing] {
    @inline override def hasNext: Boolean = false
    @inline override def next(): Nothing = Iterator.empty.next
  }

  private val emptyInstance = new EmptyIterator

  @inline def empty[G]: HybridIterator[G] = emptyInstance

  @inline def single[G](item: G): HybridIterator[G] = HybridIterator1(item)

  @inline def many[G: ClassTag](items: G*): HybridIterator[G] = {
    new ArrayWrappingIterator(items.toArray[G])
  }

  @inline def apply[G](i: JavaIterator[G]): HybridIterator[G] = {
    new HybridIterator[G] {
      @inline override def next(): G = i.next
      @inline override def hasNext: Boolean = i.hasNext
    }
  }

  @inline def apply[G](i: Iterator[G]): HybridIterator[G] = {
    new HybridIterator[G] {
      @inline override def next(): G = i.next
      @inline override def hasNext: Boolean = i.hasNext
    }
  }

  @inline def apply[G](a: Array[G]): HybridIterator[G] =
    new ArrayWrappingIterator(a)

}

final class ArrayWrappingIterator[G](a: Array[G]) extends HybridIterator[G] {

  private var i = 0

  @inline def next: G = {
    val n = a(i)
    i += 1
    n
  }

  @inline def hasNext: Boolean = i < a.length

}

final class ListWrappingIterator[G](l: List[G]) extends HybridIterator[G] {

  private var remainder = l

  @inline override def next: G = {
    val n = remainder.head
    remainder = remainder.tail
    n
  }

  @inline override def hasNext: Boolean = remainder != Nil

}

final case class HybridIterator1[G](val item: G) extends HybridIterator[G] {

  private var n = true

  @inline override def hasNext: Boolean = n

  @inline override def next(): G = {
    if (n) {
      n = false
      item
    } else {
      Iterator.empty.next
    }
  }

  @inline override def size(): Int = {
    if (n) 1 else 0
  }
}

/**
 * Both a Java and a Scala iterator.
 */
trait HybridIterator[+G]
    extends Iterator[G]
    with JavaIterator[G @uncheckedVariance] { self =>

  @inline override def map[H](f: G => H): HybridIterator[H] =
    new HybridIterator[H] {
      @inline def hasNext = self.hasNext
      @inline def next(): H = f(self.next())
    }

  @inline override def filter(p: G => Boolean): HybridIterator[G] =
    new HybridIterator[G] {
      private var nextPassingItem: G = _

      @inline
      @tailrec override def hasNext(): Boolean = {
        if (nextPassingItem != null) {
          true
        } else if (!self.hasNext) {
          false
        } else {
          val n = self.next
          if (p(n)) {
            nextPassingItem = n
            true
          } else {
            hasNext()
          }
        }
      }

      @inline override def next(): G = {
        val item = nextPassingItem
        nextPassingItem = null.asInstanceOf[G]
        item
      }

    }

}
