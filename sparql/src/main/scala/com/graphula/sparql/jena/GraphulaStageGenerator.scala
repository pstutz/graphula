package com.graphula.sparql.jena

import java.nio.ByteBuffer
import java.util.ArrayList
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaIterator
import org.apache.jena.graph.Node
import org.apache.jena.graph.Node_Concrete
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.BasicPattern
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.engine.binding.BindingRoot
import org.apache.jena.sparql.engine.main.StageGenerator
import org.lmdbjava.Txn
import com.graphula.BgpArray._
import com.graphula.sparql.Sparql
import com.graphula.util.IteratorOfIteratorsConcatenator
import com.graphula.util.ListWrappingIterator
import com.graphula.util.HybridIterator
import com.graphula.BgpArray

/**
 * Converts Jena BGP queries to the Graphula format and converts the results back to the Jena result format.
 */
final class GraphulaStageGenerator(
    sparql: Sparql,
    other: Option[StageGenerator] = None
) extends StageGenerator {

  def execute(
    basicPattern: BasicPattern,
    queryIterator: QueryIterator,
    context: ExecutionContext
  ): QueryIterator = {
    implicit val dictionary = sparql.graphula.dictionary
    implicit val txn = context.getContext.get[Txn[ByteBuffer]](Sparql.txn)
    var variables: ArrayList[Var] = new ArrayList[Var]()
    var varToIdMap = Map.empty[Var, Long] // Long to avoid conversion of wrapped index values to IDs.
    var nextVarId = -1
    @inline def getVarId(): Long = {
      val next = nextVarId
      nextVarId -= 1
      next
    }
    def nodeToId(n: Node): Option[Long] = {
      n match {
        case variable: Var =>
          val existingId = varToIdMap.get(variable)
          existingId match {
            case s @ Some(_) =>
              s
            case None =>
              val id = getVarId()
              varToIdMap += ((variable, id))
              variables.add(variable)
              Some(id)
          }
        case concrete: Node_Concrete =>
          val nodeString = Transformations.jenaConcreteNodeToString(concrete)
          val id = sparql.resolveString(nodeString)
          if (id == 0) {
            None
          } else {
            Some(id)
          }
        case other =>
          throw new UnsupportedOperationException(
            s"Node $other of type ${other.getClass} is not supported."
          )
      }
    }
    def toTriplePattern(basicPattern: BasicPattern): Option[Array[Long]] = {
      val triples = {
        basicPattern.getList match {
          case al: ArrayList[Triple] => al
          case other => new ArrayList[Triple](other)
        }
      }
      val patterns = new Array[Long](triples.size * tupleSize)
      var i = 0
      while (i < triples.size) {
        val triple = triples(i)
        val patternIndex = i * tupleSize
        val sId = nodeToId(triple.getSubject).getOrElse(return None)
        val pId = nodeToId(triple.getPredicate).getOrElse(return None)
        val oId = nodeToId(triple.getObject).getOrElse(return None)
        patterns(patternIndex + cardinalityOffset) = Long.MaxValue
        patterns(patternIndex + subjectOffset) = sId
        patterns(patternIndex + predicateOffset) = pId
        patterns(patternIndex + objectOffset) = oId
        i += 1
      }
      Some(patterns)
    }
    def applyBinding(b: Binding, bgp: Array[Long]): Array[Long] = {
      b match {
        case r: BindingRoot => bgp
        case other =>
          throw new UnsupportedOperationException(
            s"Binding $other of type ${other.getClass} is not supported."
          )
      }
    }
    val base = toTriplePattern(basicPattern)
    base match {
      case None =>
        EmptyQueryIterator
      case Some(patterns) =>
        val graphulaQueryIterator = new JenaToGraphulaQueryIteratorAdapter(
          patterns,
          queryIterator,
          variables,
          varToIdMap
        )
        val graphulaResultIterators = sparql.graphula.transaction.withCursor {
          implicit c =>
            graphulaQueryIterator.map(sparql.graphula.execute(_))
        }
        val flattenedResultIterator = new IteratorOfIteratorsConcatenator(
          graphulaResultIterators
        )
        new GraphulaToJenaQueryIteratorAdapter(
          flattenedResultIterator,
          variables,
          varToIdMap
        )
    }
  }

}
