package com.graphula.sparql.jena

import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.Iterator
import scala.collection.JavaConversions.asScalaBuffer
import org.apache.jena.atlas.io.IndentedWriter
import org.apache.jena.graph.Node
import org.apache.jena.shared.PrefixMapping
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.engine.binding.BindingRoot
import org.apache.jena.sparql.serializer.SerializationContext
import org.lmdbjava.Txn
import com.graphula.BindingArray.BindingArrayWrapper
import com.graphula.LazyBinding
import com.graphula.dictionary.Dictionary
import com.graphula.util.HybridIterator

final object JenaToGraphulaQueryIteratorAdapter {

  def applyBinding(
    bgp: Array[Long],
    jenaBinding: Binding,
    variables: ArrayList[Var],
    varToIdMap: Map[Var, Long]
  )(
    implicit
    dictionary: Dictionary,
    txn: Txn[ByteBuffer]
  ): Array[Long] = {
    jenaBinding match {
      case r: BindingRoot => bgp
      // TODO: Faster special case possible for GraphulaToJenaBindingAdapter instance?
      case jenaBinding =>
        val copy = bgp.clone
        variables.foreach { variable =>
          val binding = jenaBinding.get(variable)
          if (binding != null) {
            val bindingString =
              Transformations.jenaConcreteNodeToString(binding)
            val bindingId = dictionary(bindingString)
            var i = 0
            while (i < copy.length) {
              val v = copy(i)
              if (v == variable) {
                copy(i) = bindingId
              }
              i += 1
            }
          }
        }
        copy
    }
  }

}

final class JenaToGraphulaQueryIteratorAdapter(
    val bgp: Array[Long],
    val bindingIterator: QueryIterator,
    val variables: ArrayList[Var],
    val varToIdMap: Map[Var, Long]
)(
    implicit
    dictionary: Dictionary,
    txn: Txn[ByteBuffer]
) extends HybridIterator[Array[Long]] {

  @inline def hasNext: Boolean = bindingIterator.hasNext
  @inline def next(): Array[Long] = {
    val jenaBinding = bindingIterator.next
    JenaToGraphulaQueryIteratorAdapter.applyBinding(
      bgp,
      jenaBinding,
      variables,
      varToIdMap
    )
  }

}

//TODO: Implement
final object EmptyQueryIterator
    extends QueryIterator
    with HybridIterator[Binding] {
  def close(): Unit = {}
  def hasNext(): Boolean = false
  def next(): Binding = scala.Iterator.empty.next
  def output(w: IndentedWriter, sc: SerializationContext): Unit = ???
  def toString(pm: PrefixMapping): String = ???
  def output(iw: IndentedWriter): Unit = ???
  def cancel(): Unit = close()
  def nextBinding(): Binding = next()
}

final class GraphulaToJenaQueryIteratorAdapter(
  results: HybridIterator[Array[LazyBinding]],
  variables: ArrayList[Var],
  varToId: Map[Var, Long]
) extends QueryIterator
    with HybridIterator[Binding] {

  def close(): Unit = {}
  @inline def hasNext(): Boolean = results.hasNext
  @inline def next(): Binding = {
    val nextResult = results.next()
    new GraphulaToJenaBindingAdapter(nextResult, variables, varToId)
  }
  def output(w: IndentedWriter, sc: SerializationContext): Unit = ???
  def toString(pm: PrefixMapping): String = ???
  def output(iw: IndentedWriter): Unit = ???
  def cancel(): Unit = close()
  def nextBinding(): Binding = next()

}

final class GraphulaToJenaBindingAdapter(
    val graphulaBindings: Array[LazyBinding],
    variables: ArrayList[Var],
    varToId: Map[Var, Long]
) extends Binding {

  override def vars: Iterator[Var] = variables.iterator()

  override def contains(variable: Var): Boolean = {
    varToId.contains(variable)
  }

  override def get(variable: Var): Node = {
    val idOption = varToId.get(variable)
    idOption match {
      case None =>
        null.asInstanceOf[Node]
      case Some(varId) =>
        val bindingOption = graphulaBindings.lazyBinding(varId.toInt)
        bindingOption match {
          case Some(binding) => Transformations.bindingValueToJenaNode(binding)
          case None =>
            throw new Exception(
              s"Variable $varId was not contained in binding ${
                graphulaBindings
                  .mkString("[", ",", "]")
              }"
            )
        }
    }
  }

  /** Number of (var, value) pairs. */
  override def size(): Int = {
    variables.size()
  }

  override def isEmpty: Boolean = {
    variables.isEmpty
  }

}
