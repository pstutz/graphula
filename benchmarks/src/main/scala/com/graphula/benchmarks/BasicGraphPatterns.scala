package com.graphula.benchmarks

import org.openjdk.jmh.annotations._

import com.graphula.BgpArray.triplePatternsToLongArray
import com.graphula.Graphula
import com.graphula.TriplePattern

@State(Scope.Benchmark)
class BasicGraphPatterns {

  def load(graphula: Graphula): Unit = {
    val maxS = 20L
    val maxP = 20L
    val maxO = 20L
    graphula.transaction.withWrite { implicit txn =>
      graphula.transaction.withWriteCursor { implicit c =>
        for {
          s <- 1L to maxS
          p <- 1L to maxP
          o <- 1L to maxO
        } graphula.addToIndex(s, p, o)
      }
    }
  }

  var graphula: Graphula = _

  @Setup
  def prepare: Unit = {
    graphula = new Graphula()
    load(graphula)
  }

  @TearDown
  def check: Unit = {
    graphula.env.close()
  }

  val q: Array[Long] = List(
    TriplePattern(-1, 1, -2),
    TriplePattern(-2, 2, -3),
    TriplePattern(-3, 3, 4)
  )

  @Benchmark //@Fork(jvmArgsAppend = Array("-agentpath:/Applications/YourKit-Java-Profiler-2016.02.app/Contents/Resources/bin/mac/libyjpagent.jnilib"))
  def q1: Int = {
    graphula.transaction.withRead { implicit txn =>
      graphula.transaction.withCursor { implicit c =>
        val r = graphula.execute(q)
        r.size
      }
    }
  }

}
