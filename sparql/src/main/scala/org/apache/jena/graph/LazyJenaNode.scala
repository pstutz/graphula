package org.apache.jena.graph

import org.apache.jena.shared.PrefixMapping

import com.graphula.LazyBinding
import com.graphula.sparql.jena.Transformations
import org.apache.jena.graph.impl.LiteralLabel
import org.apache.jena.datatypes.RDFDatatype

/**
 * Node that avoids decoding if possible and can do fast equality comparisons
 * with other lazy nodes by just comparing the respective binding values.
 */
final class LazyJenaNode(val binding: LazyBinding) extends Node(binding) {
  lazy val impl: Node = {
    val decodedString = binding.decoded
    Transformations.stringToJenaNode(decodedString)
  }
  def isConcrete: Boolean = binding.value > 0
  def visitWith(v: NodeVisitor): Object = impl.visitWith(v)
  override def equals(other: Any): Boolean = {
    other match {
      case l: LazyJenaNode => binding == l.binding
      case other => impl.equals(other)
    }
  }
  override def isLiteral(): Boolean = impl.isLiteral
  override def isBlank(): Boolean = impl.isBlank
  override def isURI(): Boolean = impl.isURI
  override def isVariable(): Boolean = binding.value < 0
  override def getBlankNodeId(): BlankNodeId = impl.getBlankNodeId
  override def getBlankNodeLabel(): String = impl.getBlankNodeLabel
  override def getLiteral(): LiteralLabel = impl.getLiteral
  override def getLiteralValue(): Object = impl.getLiteralValue
  override def getLiteralLexicalForm(): String = impl.getLiteralLexicalForm
  override def getLiteralLanguage(): String = impl.getLiteralLanguage
  override def getLiteralDatatypeURI(): String = impl.getLiteralDatatypeURI
  override def getLiteralDatatype(): RDFDatatype = impl.getLiteralDatatype
  override def getLiteralIsXML(): Boolean = impl.getLiteralIsXML
  override def getIndexingValue(): Object = impl.getIndexingValue
  override def getURI(): String = impl.getURI
  override def getNameSpace(): String = impl.getNameSpace
  override def getLocalName(): String = impl.getLocalName
  override def getName(): String = impl.getName
  override def hasURI(uri: String): Boolean = impl.hasURI(uri)
  override def hashCode(): Int = binding.value.toInt
  override def matches(other: Node): Boolean = impl.matches(other)
  override def toString: String = impl.toString
  override def toString(quoting: Boolean): String = impl.toString(quoting)
  override def toString(pm: PrefixMapping): String = impl.toString(pm)
  override def toString(pm: PrefixMapping, quoting: Boolean): String =
    impl.toString(pm, quoting)
}
