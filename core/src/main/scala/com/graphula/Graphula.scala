package com.graphula

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinTask
import java.util.concurrent.RecursiveTask

import scala.util.control.NonFatal

import org.lmdbjava.Cursor
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags.MDB_NOMETASYNC
import org.lmdbjava.EnvFlags.MDB_NOSYNC
import org.lmdbjava.EnvFlags.MDB_NOTLS
import org.lmdbjava.Txn

import com.google.common.io.Files
import com.graphula.BgpArray._
import com.graphula.BindingArray._
import com.graphula.dictionary.Dictionary
import com.graphula.index.Index
import com.graphula.util.ArrayWrappingIterator
import com.graphula.util.HybridIterator
import com.graphula.util.IteratorOfIteratorsConcatenator
import com.graphula.util.ArrayOfIteratorsConcatenator
import com.graphula.util.LazyArrayOfIteratorsConcatenator
import java.util.concurrent.atomic.AtomicInteger

final object Graphula {

  val unbound: Int = 0

  def tempEnv(
    tmp: File = Files.createTempDir(),
    maxSizeInMegabytes: Long = 10000,
    durable: Boolean = false
  ): Env[ByteBuffer] = {
    val conf = Env
      .create()
      .setMapSize(maxSizeInMegabytes * 1000000)
      .setMaxDbs(3)
      .setMaxReaders(32767) // Maximum number of threads in a FJ pool.
    val flags = MDB_NOTLS :: {
      if (durable) Nil else MDB_NOSYNC :: MDB_NOMETASYNC :: Nil
    }
    conf.open(tmp, flags: _*)
  }

}

final class Graphula(
    val env: Env[ByteBuffer] = Graphula.tempEnv(),
    disableChecks: Boolean = true
) {
  if (disableChecks) {
    System.setProperty(Env.DISABLE_CHECKS_PROP, true.toString)
  }

  protected val index: Index = new Index(env)
  implicit val dictionary = new Dictionary(env)
  val transaction: Transaction = index.transaction

  @inline def createBlankNodeNamespace(): BlankNodeNamespace = {
    new BlankNodeNamespace(dictionary)
  }

  /**
   * Requires a write transaction and a write cursor for the index.
   */
  @inline
  def addStringTriple(s: String, p: String, o: String)(
    implicit
    txn: Txn[ByteBuffer],
    c: Cursor[ByteBuffer]
  ): Unit = {
    val sId = dictionary.add(s)
    val pId = dictionary.add(p)
    val oId = dictionary.add(o)
    addToIndex(sId, pId, oId)
  }

  /**
   * Requires a write transaction.
   */
  @inline
  def addToDictionary(value: String)(implicit txn: Txn[ByteBuffer]): Long = {
    dictionary.add(value)
  }

  /**
   * Requires a read transaction.
   * Returns the assigned id or 0 if no assigned id exists.
   */
  @inline def idForString(s: String)(implicit txn: Txn[ByteBuffer]): Long = {
    dictionary.apply(s)
  }

  /**
   * Requires a write cursor.
   */
  @inline
  def addToIndex(s: Long, p: Long, o: Long)(
    implicit
    c: Cursor[ByteBuffer]
  ): Unit = {
    index.addTriple(s, p, o)
  }

  def close(): Unit = {
    index.close()
    dictionary.close()
  }

  private val executor = ForkJoinPool.commonPool()

  /**
   * Returns false iff the cardinality of at least one of the patterns is 0.
   */
  @inline
  private def optimize(
    variableOccurrences: Array[Int],
    bgp: Array[Long]
  )(implicit
    c: Cursor[ByteBuffer],
    txn: Txn[ByteBuffer]): Boolean = {
    var i = 0
    var maxIndex = 0
    var maxScore: Int = 0
    var failed = false
    while (!failed && i < bgp.length) {
      val sIndex = i + subjectOffset
      val pIndex = i + predicateOffset
      val oIndex = i + objectOffset
      val s = bgp(sIndex)
      val p = bgp(pIndex)
      val o = bgp(oIndex)
      val variableId = {
        if (p < 0) p.toInt
        else if (s < 0) s.toInt
        else if (o < 0) o.toInt
        else Graphula.unbound
      }
      val cardinality = {
        val cardinalityIndex = i + cardinalityOffset
        bgp(cardinalityIndex) match {
          case Long.MaxValue =>
            val indexS = math.max(s, 0)
            val indexP = math.max(p, 0)
            val indexO = math.max(o, 0)
            val updatedCardinality = if (variableId == Graphula.unbound) {
              if (index.exists(indexS, indexP, indexO)) {
                1
              } else {
                0
              }
            } else {
              index.valueCount(indexS, indexP, indexO)
            }
            if (updatedCardinality == 0) {
              failed = true
            } else {
              bgp(cardinalityIndex) = updatedCardinality
            }
            updatedCardinality
          case other => other
        }
      }
      if (!failed && maxScore < Int.MaxValue) {
        val score = {
          val occurrences = if (variableId == Graphula.unbound) {
            0
          } else {
            val variableOccurrencesIndex = indexForVariableId(variableId)
            variableOccurrences(variableOccurrencesIndex)
          }
          occurrences + java.lang.Long.numberOfLeadingZeros(cardinality)
        }
        if (score > maxScore) {
          maxScore = score
          maxIndex = i
        }
      }
      i += tupleSize
    }
    if (!failed && maxIndex != 0) {
      bgp.swap(0, maxIndex)
    }
    failed
  }

  /**
   * Requires a read transaction.
   */
  @inline
  def execute(bgp: Array[Long])(
    implicit
    c: Cursor[ByteBuffer],
    txn: Txn[ByteBuffer]
  ): HybridIterator[Array[LazyBinding]] = {
    execute(bgp, BindingArray.empty, bgp.variableCoOccurrences)
  }

  /**
   * Requires a read transaction.
   */
  @inline
  def execute(
    bgp: Array[Long],
    bindings: Array[LazyBinding],
    variableOccurrences: Array[Int]
  )(
    implicit
    c: Cursor[ByteBuffer],
    txn: Txn[ByteBuffer]
  ): HybridIterator[Array[LazyBinding]] = {
    if (bgp.length == 0) {
      HybridIterator.single(bindings)
    } else {
      val failed = if (bgp.length > 1) {
        optimize(variableOccurrences, bgp)
      } else {
        false
      }
      if (failed) {
        HybridIterator.empty
      } else {
        val cardinality = bgp.headCardinality
        if (cardinality == 0) {
          HybridIterator.empty
        } else {
          val s = bgp.headSubject
          val p = bgp.headPredicate
          val o = bgp.headObject
          val indexS = math.max(s, 0)
          val indexP = math.max(p, 0)
          val indexO = math.max(o, 0)
          if (bgp.isHeadFullyBound) {
            // Existence check
            if (cardinality == 0) {
              HybridIterator.empty
            } else if (cardinality == 1 || index.exists(indexS, indexP, indexO)) {
              if (bgp.unboundPositions == 0) {
                // Existence checks without cardinality 1 are guaranteed to be eliminated immediately.
                HybridIterator.single(bindings)
              } else {
                val updatedBgp = bgp.bgpTail
                val updatedOccurrences = updatedBgp.variableCoOccurrencesWithSize(variableOccurrences.length)
                execute(updatedBgp, bindings, updatedOccurrences)
              }
            } else {
              HybridIterator.empty
            }
          } else {
            // Bind the next variable which has to exist, because the head pattern is not fully bound
            val variableId = {
              if (p < 0) p.toInt
              else if (s < 0) s.toInt
              else if (o < 0) o.toInt
              else Graphula.unbound
            }
            if (variableId == Graphula.unbound) {
              // We cannot bind this index pattern in a BGP, no results
              HybridIterator.empty
            } else {
              val variableBindings = index.values(indexS, indexP, indexO)
              // Compute updated bindings and BGPs for all variable bindings
              val updatedBgpArray = bgp.bindMany(variableId, variableBindings)
              val updatedBindingsArray =
                bindings.addBindings(variableId, variableBindings)
              updatedBgpArray.length match {
                case 0 =>
                  HybridIterator.empty // There are no bindings for the variable, pattern matching has failed for this BGP
                case 1 =>
                  val updatedOccurrences = updatedBgpArray(0).variableCoOccurrencesWithSize(variableOccurrences.length)
                  execute(updatedBgpArray(0), updatedBindingsArray(0), updatedOccurrences) // 1 result, recurse
                case many =>
                  // Many results, consider spawning parallel execution steps
                  val parallelism = updatedBgpArray(0).unboundPositions
                  if (parallelism <= 1) {
                    // Not enough parallelism remaining to spawn parallel executions, execute sequentially instead
                    if (updatedBgpArray(0).length == 0) {
                      // No more patterns to match, we directly return the wrapped bindings array as a result iterator
                      new ArrayWrappingIterator(updatedBindingsArray)
                    } else {
                      // Sequentially match all remaining BGPs and extend their respective bindings
                      val resultArray =
                        new Array[HybridIterator[Array[LazyBinding]]](many)
                      val updatedOccurrences = updatedBgpArray(0).variableCoOccurrencesWithSize(variableOccurrences.length)
                      var i = 0
                      while (i < many) {
                        resultArray(i) =
                          execute(updatedBgpArray(i), updatedBindingsArray(i), updatedOccurrences)
                        i += 1
                      }
                      new ArrayOfIteratorsConcatenator(resultArray)
                    }
                  } else {
                    //Fork the execution and match all but one remaining BGP with parallel F/J tasks
                    val taskArray =
                      new Array[ForkJoinTask[HybridIterator[Array[LazyBinding]]]](
                        many - 1
                      )
                    var i = 0
                    val updatedOccurrences = updatedBgpArray(0).variableCoOccurrencesWithSize(variableOccurrences.length)
                    while (i < taskArray.length) {
                      taskArray(i) = executor.submit(
                        new ExecutionStep(
                          updatedBgpArray(i),
                          updatedBindingsArray(i),
                          updatedOccurrences
                        )
                      )
                      i += 1
                    }
                    val lastIndex = many - 1
                    val lastResults = execute(
                      updatedBgpArray(lastIndex),
                      updatedBindingsArray(lastIndex),
                      updatedOccurrences
                    )
                    val arrayOfLazyPartialResults =
                      new Array[() => HybridIterator[Array[LazyBinding]]](many)
                    arrayOfLazyPartialResults(many - 1) = () => lastResults
                    i = 0
                    while (i < taskArray.length) {
                      arrayOfLazyPartialResults(i) = taskArray(i).join _
                      i += 1
                    }
                    new LazyArrayOfIteratorsConcatenator(arrayOfLazyPartialResults)
                  }
              }
            }
          }
        }
      }
    }
  }

  private final class ExecutionStep(
    patterns: Array[Long],
    binding: Array[LazyBinding],
    variableOccurrences: Array[Int]
  )(
    implicit
    txn: Txn[ByteBuffer]
  )
      extends RecursiveTask[HybridIterator[Array[LazyBinding]]] {
    @inline protected def compute(): HybridIterator[Array[LazyBinding]] = {
      try {
        transaction.withCursor { implicit c =>
          execute(patterns, binding, variableOccurrences)
        }
      } catch {
        case NonFatal(e) =>
          e.printStackTrace()
          throw e
      }
    }
  }

  /**
   * Requires a read transaction.
   */
  @inline
  def count(
    bgp: Array[Long],
    variableOccurrences: Array[Int]
  )(
    implicit
    c: Cursor[ByteBuffer],
    txn: Txn[ByteBuffer]
  ): Long = {
    if (bgp.length == 0) {
      1
    } else {
      val failed = if (bgp.length > 1) {
        optimize(variableOccurrences, bgp)
      } else {
        false
      }
      if (failed) {
        0
      } else {
        val cardinality = bgp.headCardinality
        if (cardinality == 0) {
          0
        } else if (bgp.length == 1 && bgp.unboundPositions == 1) {
          // Cardinality equals number of results.
          cardinality
        } else {
          val s = bgp.headSubject
          val p = bgp.headPredicate
          val o = bgp.headObject
          val indexS = math.max(s, 0)
          val indexP = math.max(p, 0)
          val indexO = math.max(o, 0)
          if (s > 0 && p > 0 && o > 0) {
            // Existence check
            if (cardinality == 0) {
              0
            } else if (cardinality == 1 || index.exists(indexS, indexP, indexO)) {
              if (bgp.unboundPositions == 0) {
                // Existence checks without cardinality 1 are guaranteed to be eliminated immediately.
                1
              } else {
                val updatedBgp = bgp.bgpTail
                val updatedOccurrences = updatedBgp.variableCoOccurrencesWithSize(variableOccurrences.length)
                count(updatedBgp, updatedOccurrences)
              }
            } else {
              0
            }
          } else {
            // Bind the next variable which has to exist, because the head pattern is not fully bound
            val variableId = {
              if (p < 0) p.toInt
              else if (s < 0) s.toInt
              else if (o < 0) o.toInt
              else Graphula.unbound
            }
            if (variableId == Graphula.unbound) {
              // We cannot bind this index pattern in a BGP, no results
              0
            } else {
              val variableBindings = index.values(indexS, indexP, indexO)
              // Compute updated bindings and BGPs for all variable bindings
              val updatedBgpArray = bgp.bindMany(variableId, variableBindings)
              updatedBgpArray.length match {
                case 0 => 0 // There are no bindings for the variable, pattern matching has failed for this BGP
                case 1 =>
                  val updatedOccurrences = updatedBgpArray(0).variableCoOccurrencesWithSize(variableOccurrences.length)
                  count(updatedBgpArray(0), updatedOccurrences) // 1 result, recurse
                case many =>
                  // Many results, consider spawning parallel execution steps
                  val parallelism = updatedBgpArray(0).unboundPositions
                  if (parallelism <= 1) {
                    // Not enough parallelism remaining to spawn parallel executions, execute sequentially instead
                    if (updatedBgpArray(0).length == 0) {
                      // No more patterns to match, we directly return the wrapped bindings array as a result iterator
                      variableBindings.remaining
                    } else {
                      // Sequentially match all remaining BGPs and extend their respective bindings
                      val updatedOccurrences = updatedBgpArray(0).variableCoOccurrencesWithSize(variableOccurrences.length)
                      var i = 0
                      var resultCount: Long = 0
                      while (i < many) {
                        resultCount += count(updatedBgpArray(i), updatedOccurrences)
                        i += 1
                      }
                      resultCount
                    }
                  } else {
                    //Fork the execution and match all but one remaining BGP with parallel F/J tasks
                    val taskArray = new Array[ForkJoinTask[Long]](many - 1)
                    var i = 0
                    val updatedOccurrences = updatedBgpArray(0).variableCoOccurrencesWithSize(variableOccurrences.length)
                    while (i < taskArray.length) {
                      taskArray(i) = executor.submit(
                        new CountingStep(
                          updatedBgpArray(i),
                          updatedOccurrences
                        )
                      )
                      i += 1
                    }
                    val lastIndex = many - 1
                    val lastResults = count(
                      updatedBgpArray(lastIndex),
                      updatedOccurrences
                    )
                    var sum = lastResults
                    i = 0
                    while (i < taskArray.length) {
                      sum += taskArray(i).join
                      i += 1
                    }
                    sum
                  }
              }
            }
          }
        }
      }
    }
  }

  private final class CountingStep(
    patterns: Array[Long],
    variableOccurrences: Array[Int]
  )(
    implicit
    txn: Txn[ByteBuffer]
  )
      extends RecursiveTask[Long] {
    @inline protected def compute(): Long = {
      try {
        transaction.withCursor { implicit c =>
          count(patterns, variableOccurrences)
        }
      } catch {
        case NonFatal(e) =>
          e.printStackTrace()
          throw e
      }
    }
  }

  //  /**
  //   * Requires a read transaction.
  //   */
  //  @inline
  //  def execute(bgp: Array[Long])(
  //    implicit
  //    c: Cursor[ByteBuffer],
  //    txn: Txn[ByteBuffer]
  //  ): HybridIterator[Array[LazyBinding]] = {
  //    execute(bgp, BindingArray.empty, bgp.variableCoOccurrences)
  //  }
  //
  //  /**
  //   * Requires a read transaction.
  //   */
  //  @inline
  //  def randomWalk(
  //    start: Long,
  //    predicate: Long,
  //    tickets: Long,
  //
  //    bindings: Array[LazyBinding],
  //    variableOccurrences: Array[Int]
  //  )(
  //    implicit
  //    c: Cursor[ByteBuffer],
  //    txn: Txn[ByteBuffer]
  //  ): HybridIterator[Array[LazyBinding]] = {
  //    if (bgp.length == 0) {
  //      HybridIterator.single(bindings)
  //    } else {
  //      val failed = if (bgp.length > 1) {
  //        optimize(variableOccurrences, bgp)
  //      } else {
  //        false
  //      }
  //      if (failed) {
  //        HybridIterator.empty
  //      } else {
  //        val cardinality = bgp.headCardinality
  //        if (cardinality == 0) {
  //          HybridIterator.empty
  //        } else {
  //          val s = bgp.headSubject
  //          val p = bgp.headPredicate
  //          val o = bgp.headObject
  //          val indexS = math.max(s, 0)
  //          val indexP = math.max(p, 0)
  //          val indexO = math.max(o, 0)
  //          if (bgp.isHeadFullyBound) {
  //            // Existence check
  //            if (cardinality == 0) {
  //              HybridIterator.empty
  //            } else if (cardinality == 1 || index.exists(indexS, indexP, indexO)) {
  //              if (bgp.unboundPositions == 0) {
  //                // Existence checks without cardinality 1 are guaranteed to be eliminated immediately.
  //                HybridIterator.single(bindings)
  //              } else {
  //                val updatedBgp = bgp.bgpTail
  //                val updatedOccurrences = updatedBgp.variableCoOccurrencesWithSize(variableOccurrences.length)
  //                execute(updatedBgp, bindings, updatedOccurrences)
  //              }
  //            } else {
  //              HybridIterator.empty
  //            }
  //          } else {
  //            // Bind the next variable which has to exist, because the head pattern is not fully bound
  //            val variableId = {
  //              if (p < 0) p.toInt
  //              else if (s < 0) s.toInt
  //              else if (o < 0) o.toInt
  //              else Graphula.unbound
  //            }
  //            if (variableId == Graphula.unbound) {
  //              // We cannot bind this index pattern in a BGP, no results
  //              HybridIterator.empty
  //            } else {
  //              val variableBindings = index.values(indexS, indexP, indexO)
  //              // Compute updated bindings and BGPs for all variable bindings
  //              val updatedBgpArray = bgp.bindMany(variableId, variableBindings)
  //              val updatedBindingsArray =
  //                bindings.addBindings(variableId, variableBindings)
  //              updatedBgpArray.length match {
  //                case 0 =>
  //                  HybridIterator.empty // There are no bindings for the variable, pattern matching has failed for this BGP
  //                case 1 =>
  //                  val updatedOccurrences = updatedBgpArray(0).variableCoOccurrencesWithSize(variableOccurrences.length)
  //                  execute(updatedBgpArray(0), updatedBindingsArray(0), updatedOccurrences) // 1 result, recurse
  //                case many =>
  //                  // Many results, consider spawning parallel execution steps
  //                  val parallelism = updatedBgpArray(0).unboundPositions
  //                  if (parallelism <= 1) {
  //                    // Not enough parallelism remaining to spawn parallel executions, execute sequentially instead
  //                    if (updatedBgpArray(0).length == 0) {
  //                      // No more patterns to match, we directly return the wrapped bindings array as a result iterator
  //                      new ArrayWrappingIterator(updatedBindingsArray)
  //                    } else {
  //                      // Sequentially match all remaining BGPs and extend their respective bindings
  //                      val resultArray =
  //                        new Array[HybridIterator[Array[LazyBinding]]](many)
  //                      val updatedOccurrences = updatedBgpArray(0).variableCoOccurrencesWithSize(variableOccurrences.length)
  //                      var i = 0
  //                      while (i < many) {
  //                        resultArray(i) =
  //                          execute(updatedBgpArray(i), updatedBindingsArray(i), updatedOccurrences)
  //                        i += 1
  //                      }
  //                      new ArrayOfIteratorsConcatenator(resultArray)
  //                    }
  //                  } else {
  //                    //Fork the execution and match all but one remaining BGP with parallel F/J tasks
  //                    val taskArray =
  //                      new Array[ForkJoinTask[HybridIterator[Array[LazyBinding]]]](
  //                        many - 1
  //                      )
  //                    var i = 0
  //                    val updatedOccurrences = updatedBgpArray(0).variableCoOccurrencesWithSize(variableOccurrences.length)
  //                    while (i < taskArray.length) {
  //                      taskArray(i) = executor.submit(
  //                        new ExecutionStep(
  //                          updatedBgpArray(i),
  //                          updatedBindingsArray(i),
  //                          updatedOccurrences
  //                        )
  //                      )
  //                      i += 1
  //                    }
  //                    val lastIndex = many - 1
  //                    val lastResults = execute(
  //                      updatedBgpArray(lastIndex),
  //                      updatedBindingsArray(lastIndex),
  //                      updatedOccurrences
  //                    )
  //                    val arrayOfLazyPartialResults =
  //                      new Array[() => HybridIterator[Array[LazyBinding]]](many)
  //                    arrayOfLazyPartialResults(many - 1) = () => lastResults
  //                    i = 0
  //                    while (i < taskArray.length) {
  //                      arrayOfLazyPartialResults(i) = taskArray(i).join _
  //                      i += 1
  //                    }
  //                    new LazyArrayOfIteratorsConcatenator(arrayOfLazyPartialResults)
  //                  }
  //              }
  //            }
  //          }
  //        }
  //      }
  //    }
  //  }

}
