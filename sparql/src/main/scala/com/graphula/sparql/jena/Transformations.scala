package com.graphula.sparql.jena

import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.BlankNodeId
import org.apache.jena.graph.LazyJenaNode
import org.apache.jena.graph.{ Node => JenaNode }
import org.apache.jena.graph.NodeFactory
import org.apache.jena.graph.{ Node_URI => JenaUri }
import org.apache.jena.graph.impl.LiteralLabelFactory
import org.semanticweb.yars.nx.{ Literal => NxLiteral }
import org.semanticweb.yars.nx.{ Resource => NxResource }
import com.graphula.LazyBinding
import org.apache.jena.graph.Node_Literal

final object Transformations {

  def nxResourceToString(resource: NxResource): String = {
    resource.getLabel
  }

  def nxLiteralToString(literal: NxLiteral): String = {
    literal.toString
  }

  // TODO: Properly handle empty strings.
  // TODO: Simplify
  def stringToJenaNode(s: String): JenaNode = {
    assert(s != null, "String cannot be null")
    assert(!s.isEmpty, "String cannot be empty")
    s.head match {
      case 'h' => // Shortcut http prefix.
        NodeFactory.createURI(s)
      case '"' =>
        if (s.last == '"') {
          // Simple case, no type or language tag.
          NodeFactory.createLiteral(s.substring(1, s.length - 1))
        } else {
          val tagIndex = s.lastIndexOf('@')
          val tpeIndex = s.lastIndexOf('^')
          if (tpeIndex > tagIndex) {
            // Yes, we have a type.
            val typeName = s.substring(tpeIndex + 1)
            val dt = TypeMapper.getInstance.getTypeByName(typeName)
            val lex = s.substring(1, tpeIndex - 2)
            val label = LiteralLabelFactory.create(lex, dt)
            NodeFactory.createLiteral(label)
          } else if (tagIndex > 0) {
            val lex = s.substring(1, tagIndex - 1)
            val lang = s.substring(tagIndex + 1)
            val label = LiteralLabelFactory.create(lex, lang)
            NodeFactory.createLiteral(label)
          } else {
            throw new UnsupportedOperationException(
              s"Could not convert literal $s to a node."
            )
          }
        }
      case c if (c == '-' || c.isDigit) =>
        val dt = TypeMapper.getInstance.getTypeByName(
          "http://www.w3.org/2001/XMLSchema#integer"
        )
        val label = LiteralLabelFactory.create(s, dt)
        NodeFactory.createLiteral(label)
      case '<' =>
        NodeFactory.createLiteral(s.tail, null, true)
      case '_' =>
        NodeFactory.createBlankNode(BlankNodeId.create(s.tail))
      case c if c.isLetter =>
        NodeFactory.createURI(s)
      case other: Char =>
        throw new UnsupportedOperationException(
          s"Encoded string $s could not be decoded."
        )
    }
  }

  def bindingValueToJenaNode(value: LazyBinding): JenaNode = {
    new LazyJenaNode(value)
  }

  def jenaConcreteNodeToString(node: JenaNode): String = {
    node match {
      case uri: JenaUri => uri.toString
      case literal: Node_Literal => literal.toString
      case other =>
        throw new UnsupportedOperationException(
          s"Could not map node $other of type ${other.getClass} to a string."
        )
    }

  }

}
