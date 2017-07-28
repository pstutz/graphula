package com.graphula

import java.nio.ByteBuffer

import org.lmdbjava.Txn
import java.nio.LongBuffer
import com.graphula.dictionary.Dictionary

final object BindingArray {

  val unbound = 0

  val emptyArrayOfBindings = new Array[Array[LazyBinding]](0)

  val empty: Array[LazyBinding] = Array.empty[LazyBinding]

  @inline def indexForVariableId(variableId: Int): Int = {
    (-variableId) - 1
  }

  @inline def variableIdForIndex(index: Int): Int = {
    -(index + 1)
  }

  implicit final class BindingArrayWrapper(
    val bindingArray: Array[LazyBinding]
  )
      extends AnyVal {

    def asIntLongMap: Map[Int, Long] = {
      bindingArray.zipWithIndex.map {
        case (binding, index) =>
          variableIdForIndex(index) -> binding.value
      }.toMap
    }

    /**
     * Saves the Option allocation.
     */
    @inline def getUnboundAsZero(variableId: Int): Long = {
      val index = indexForVariableId(variableId)
      if (index < bindingArray.length) {
        val perhapsB = bindingArray(index)
        if (perhapsB != null) {
          perhapsB.value
        } else {
          unbound
        }
      } else {
        unbound
      }
    }

    @inline def get(variableId: Int): Option[Long] = {
      val index = indexForVariableId(variableId)
      if (index < bindingArray.length) {
        Option(bindingArray(index)).map(_.value)
      } else {
        None
      }
    }

    @inline def decoded(variableId: Int): Option[String] = {
      val index = indexForVariableId(variableId)
      if (index < bindingArray.length) {
        Option(bindingArray(index)).map(_.decoded)
      } else {
        None
      }
    }

    @inline def lazyBinding(variableId: Int): Option[LazyBinding] = {
      val index = indexForVariableId(variableId)
      if (index < bindingArray.length) {
        Option(bindingArray(index))
      } else {
        None
      }
    }

    @inline
    def addBinding(variableId: Int, binding: LazyBinding)(
      implicit
      txn: Txn[ByteBuffer]
    ): Array[LazyBinding] = {
      val index = indexForVariableId(variableId)
      val copyOnWrite = if (index < bindingArray.length) {
        bindingArray.clone()
      } else {
        val minRequiredLength = index + 1
        val newBinding = new Array[LazyBinding](minRequiredLength)
        System.arraycopy(bindingArray, 0, newBinding, 0, bindingArray.length)
        newBinding
      }
      copyOnWrite(index) = binding
      copyOnWrite
    }

    @inline
    def addBindings(variableId: Int, bindings: LongBuffer)(
      implicit
      dictionary: Dictionary,
      txn: Txn[ByteBuffer]
    ): Array[Array[LazyBinding]] = {
      if (!bindings.hasRemaining) {
        emptyArrayOfBindings
      } else {
        val index = indexForVariableId(variableId)
        val originalCopy = if (index < bindingArray.length) {
          bindingArray.clone
        } else {
          val minRequiredLength = index + 1
          val newBinding = new Array[LazyBinding](minRequiredLength)
          System.arraycopy(bindingArray, 0, newBinding, 0, bindingArray.length)
          newBinding
        }
        val numberOfBindings = bindings.remaining
        val resultArray = new Array[Array[LazyBinding]](numberOfBindings)
        resultArray(0) = originalCopy
        var i = 1
        while (i < numberOfBindings) {
          resultArray(i) = originalCopy.clone
          i += 1
        }
        i = 0
        while (i < numberOfBindings) {
          resultArray(i)(index) = new LazyBinding(bindings.get(i))
          i += 1
        }
        resultArray
      }
    }

  }

}
