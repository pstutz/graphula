package com.graphula

import java.nio.ByteBuffer

import org.lmdbjava.Txn

import com.graphula.dictionary.Dictionary

/**
 * This design is chosen because we never want to decode something before it is needed,
 * but once we decode it the decoded string should be shared among all usages of this instance.
 */
final class LazyBinding(
    val value: Long,
    existingDecoded: String = null.asInstanceOf[String]
)(
    implicit
    dictionary: Dictionary,
    txn: Txn[ByteBuffer]
) {

  lazy val decoded: String = {
    if (existingDecoded != null) {
      existingDecoded
    } else {
      dictionary(value)
    }
  }

  override def equals(other: Any): Boolean = {
    if (other == null) {
      false
    } else {
      other match {
        case l: LazyBinding => l.value == value
        case other => false
      }
    }
  }

  override def hashCode: Int = {
    value.hashCode
  }

}
