package com.graphula.sparql.jena

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import org.apache.jena.atlas.io.IndentedWriter
import org.apache.jena.graph.Triple
import org.apache.jena.query.ARQ
import org.apache.jena.query.Dataset
import org.apache.jena.query.DatasetFactory
import org.apache.jena.query.Query
import org.apache.jena.query.QueryExecException
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QuerySolution
import org.apache.jena.query.ResultSet
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.shared.PrefixMapping
import org.apache.jena.sparql.ARQConstants
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.jena.sparql.core.Quad
import org.apache.jena.sparql.core.ResultBinding
import org.apache.jena.sparql.engine.QueryEngineFactory
import org.apache.jena.sparql.engine.QueryEngineRegistry
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.engine.binding.BindingRoot
import org.apache.jena.sparql.serializer.SerializationContext
import org.apache.jena.sparql.util.Context
import org.lmdbjava.Txn

import com.graphula.Transaction
import com.graphula.sparql.Sparql
import com.graphula.util.HybridIterator
import org.apache.jena.atlas.lib.Lib

// TODO: Replace as much as possible with existing Jena implementations and maintain performance
final object GraphulaQueryExecutionFactory {

  def create(query: Query, sparql: Sparql): GraphulaExecution = {
    val transaction = sparql.transaction
    query.setResultVars()
    val context = ARQ.getContext
    val dataset = DatasetFactory.create(sparql.model)
    val dsg = dataset.asDatasetGraph()
    val eOption = findEngine(query, dsg, context)
    eOption match {
      case None =>
        throw new UnsupportedOperationException(
          s"Could not find a QueryEngineFactory for query $query."
        )
      case Some(engine) =>
        new GraphulaExecution(
          transaction,
          query,
          dataset,
          dsg,
          context,
          engine
        )
    }
  }

  def findEngine(
    query: Query,
    dataset: DatasetGraph,
    context: Context
  ): Option[QueryEngineFactory] = {
    Option(QueryEngineRegistry.get().find(query, dataset, context))
  }

}

final class HybridResultSet(
  resultVars: java.util.List[String],
  m: Model,
  iter: java.util.Iterator[Binding]
)
    extends ResultSet
    with HybridIterator[QuerySolution] {

  private var resultCount = 0

  @inline override def hasNext: Boolean = iter.hasNext
  @inline override def next: QuerySolution = new ResultBinding(m, nextBinding)
  @inline override def nextSolution(): QuerySolution = next
  @inline override def nextBinding(): Binding = {
    resultCount += 1
    iter.next()
  }
  @inline override def size(): Int = {
    while (hasNext) {
      nextBinding
    }
    resultCount
  }

  override def getResourceModel: Model = m
  override def getResultVars: java.util.List[String] = resultVars
  override def getRowNumber(): Int = resultCount

}

final class TransactionReleasingIterator(
  i: QueryIterator,
  txn: Txn[ByteBuffer],
  transaction: Transaction
)
    extends QueryIterator {

  @inline override def hasNext: Boolean = {
    if (isClosed) {
      false
    } else {
      if (i.hasNext) {
        true
      } else {
        close()
        false
      }
    }
  }
  @inline override def next(): Binding = nextBinding()
  @inline override def nextBinding(): Binding = i.nextBinding()

  private var isClosed = false

  override def close(): Unit = {
    if (!isClosed) {
      transaction.releaseRead(txn)
      isClosed = true
    }
  }

  override def cancel(): Unit = {
    i.cancel()
    close()
  }

  // TODO: Implement.
  def toString(pm: PrefixMapping): String = ???
  def output(writer: IndentedWriter): Unit = ???
  def output(writer: IndentedWriter, context: SerializationContext): Unit = {
    writer.print(Lib.className(this))
  }
}

/**
 * Placing the transaction in the execution context.
 * We can only do this once the context has been copied by QueryExecutionBase.init()
 */
final class GraphulaExecution(
    transaction: Transaction,
    query: Query,
    dataset: Dataset,
    dsg: DatasetGraph,
    originialContext: Context,
    engine: QueryEngineFactory
) extends QueryExecution {

  lazy val txn = transaction.read()

  lazy val context: Context = {
    val copy = originialContext.copy
    copy.put(Sparql.txn, txn)
    Option(dataset.getContext).foreach(copy.putAll(_))
    copy.put(ARQConstants.sysCurrentQuery, query)
    copy
  }

  private var closed: Boolean = false
  private var queryIterator: QueryIterator = _

  // TODO: Check that result set has not been closed before usage
  override def setInitialBinding(binding: QuerySolution): Unit = unsupported
  override def getDataset(): Dataset = dataset
  override def getContext(): Context = context
  override def getQuery(): Query = query

  override def execSelect(): HybridResultSet = {
    if (!query.isSelectType()) {
      throw new QueryExecException(
        s"Unsupported attempt to have a ResultSet from query $query."
      );
    }
    def toResultSet(i: QueryIterator): HybridResultSet = {
      val model = Option(dataset)
        .map(_.getDefaultModel)
        .getOrElse(ModelFactory.createDefaultModel())
      new HybridResultSet(query.getResultVars(), model, i)
    }
    val plan = engine.create(query, dsg, BindingRoot.create(), context)
    val plainIterator = plan.iterator()
    queryIterator =
      new TransactionReleasingIterator(plainIterator, txn, transaction)
    toResultSet(queryIterator)
  }

  override def abort(): Unit = {
    close()
  }

  override def close(): Unit = {
    if (!closed) {
      Option(queryIterator).foreach(_.close())
      closed = true
    }
  }

  override def isClosed(): Boolean = {
    closed
  }

  private def unsupported =
    throw new UnsupportedOperationException(
      "GraphulaExecution does not support this operation."
    )
  override def execConstruct(): Model = unsupported
  override def execConstruct(model: Model): Model = unsupported
  override def execConstructTriples(): java.util.Iterator[Triple] = unsupported
  override def execConstructQuads(): java.util.Iterator[Quad] = unsupported
  override def execConstructDataset(): Dataset = unsupported
  override def execConstructDataset(dataset: Dataset): Dataset = unsupported
  override def execDescribe(): Model = unsupported
  override def execDescribe(model: Model): Model = unsupported
  override def execDescribeTriples(): java.util.Iterator[Triple] = unsupported
  override def execAsk(): Boolean = unsupported
  override def setTimeout(timeout: Long, timeoutUnits: TimeUnit): Unit =
    unsupported
  override def setTimeout(timeout: Long): Unit = unsupported
  override def setTimeout(
    timeout1: Long,
    timeUnit1: TimeUnit,
    timeout2: Long,
    timeUnit2: TimeUnit
  ): Unit = unsupported
  override def setTimeout(timeout1: Long, timeout2: Long) = unsupported
  override def getTimeout1(): Long = unsupported
  override def getTimeout2(): Long = unsupported

}
