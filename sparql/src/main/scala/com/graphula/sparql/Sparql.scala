package com.graphula.sparql

import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer

import org.apache.jena.graph.Triple
import org.apache.jena.graph.impl.GraphBase
import org.apache.jena.query.ARQ
import org.apache.jena.query.{ Query => JenaQuery }
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.sparql.engine.main.StageGenerator
import org.apache.jena.sparql.util.Symbol
import org.apache.jena.util.iterator.ExtendedIterator
import org.lmdbjava.Txn
import org.semanticweb.yars.nx.BNode
import org.semanticweb.yars.nx.{ Literal => NxLiteral }
import org.semanticweb.yars.nx.{ Node => NxNode }
import org.semanticweb.yars.nx.{ Resource => NxResource }
import org.semanticweb.yars.nx.parser.NxParser

import com.graphula.Graphula
import com.graphula.Transaction
import com.graphula.sparql.jena.GraphulaStageGenerator
import com.graphula.sparql.jena.HybridResultSet
import com.graphula.sparql.jena.GraphulaQueryExecutionFactory
import com.graphula.sparql.jena.Transformations

final object Sparql {

  val txn = Symbol.create("txn")

}

final class Sparql(val graphula: Graphula) extends GraphBase {

  initialize()

  def initialize(): Unit = {
    // Set GraphulaStageGenerator as the default Jena stage generator.
    val s = ARQ.getContext.get[StageGenerator](ARQ.stageGenerator)
    val graphulaStageGenerator = Option(s) match {
      case None => new GraphulaStageGenerator(this, None)
      case Some(gsg: GraphulaStageGenerator) => gsg
      case Some(other: StageGenerator) =>
        new GraphulaStageGenerator(this, Some(other))
    }
    ARQ.getContext.set(ARQ.stageGenerator, graphulaStageGenerator)
    ARQ.getContext.set(ARQ.enableExecutionTimeLogging, false)
  }

  val model: Model = ModelFactory.createModelForGraph(this)
  def transaction: Transaction = graphula.transaction

  def execute(queryString: String): HybridResultSet = {
    val query = QueryFactory.create(queryString)
    executeJenaQuery(query)
  }

  def executeJenaQuery(query: JenaQuery): HybridResultSet = {
    val execution = GraphulaQueryExecutionFactory.create(query, this)
    execution.execSelect
  }

  /**
   * Requires a read transaction.
   * Returns the assigned id or 0 if no assigned id exists.
   */
  def resolveString(s: String)(implicit txn: Txn[ByteBuffer]): Long = {
    graphula.idForString(s)
  }

  private val nxp = new NxParser()

  //TODO: Ensure string encodings are Jena-compatible.
  def loadNtriples(f: File): Unit = {
    val is = new FileInputStream(f)
    val bns = graphula.createBlankNodeNamespace()
    graphula.transaction.withWrite { implicit txn =>
      graphula.transaction.withWriteCursor { implicit c =>
        def encode(nxNode: NxNode): Long = nxNode match {
          case bNode: BNode => bns.getBlankNodeId(bNode.getLabel)
          case literal: NxLiteral =>
            graphula.addToDictionary(
              Transformations.nxLiteralToString(literal)
            )
          case resource: NxResource =>
            graphula.addToDictionary(
              Transformations.nxResourceToString(resource)
            )
        }
        nxp.parse(is)
        var count = 0
        val startTime = System.currentTimeMillis
        while (nxp.hasNext()) {
          val triple = nxp.next()
          val s = encode(triple(0))
          val p = encode(triple(1))
          val o = encode(triple(2))
          graphula.addToIndex(s, p, o)
          count += 1
          if (count % 1000000 == 0) {
            val current = System.currentTimeMillis
            val delta = current - startTime
            val deltaInSeconds = delta / 1000
            println(s"Triples: $count time: $deltaInSeconds seconds => ${count / deltaInSeconds} triples/second")
          }
        }
      }
    }
  }

  override def performAdd(t: Triple): Unit = {
    graphula.transaction.withWrite { implicit txn =>
      graphula.transaction.withWriteCursor { implicit c =>
        val subjectString =
          Transformations.jenaConcreteNodeToString(t.getSubject)
        val predicateString =
          Transformations.jenaConcreteNodeToString(t.getPredicate)
        val objectString =
          Transformations.jenaConcreteNodeToString(t.getObject)
        graphula.addStringTriple(subjectString, predicateString, objectString)
      }
    }
  }

  // TODO: Implement with normal execution?
  def graphBaseFind(triple: Triple): ExtendedIterator[Triple] = {
    throw new UnsupportedOperationException(
      "graphBaseFind is very inefficient and should not be used."
    )
  }

}
