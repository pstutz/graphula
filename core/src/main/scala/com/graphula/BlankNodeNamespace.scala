package com.graphula

import com.graphula.dictionary.Dictionary

final class BlankNodeNamespace(dictionary: Dictionary) {

  private var cachedBnAssignments = Map.empty[String, Long]

  /**
   * Requires a read transaction.
   */
  def getBlankNodeId(id: String): Long = {
    val existingIdOption = cachedBnAssignments.get(id)
    existingIdOption match {
      case Some(existingId) =>
        existingId
      case None =>
        val blankNodeId = dictionary.getBlankNode()
        cachedBnAssignments += id -> blankNodeId
        blankNodeId
    }
  }

}
