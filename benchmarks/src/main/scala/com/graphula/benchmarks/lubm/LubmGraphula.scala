package com.graphula.benchmarks.lubm

import org.openjdk.jmh.annotations._
import scala.collection.JavaConversions._
import java.io.File
import Lubm._
import com.graphula.sparql.Sparql
import org.lmdbjava.Env
import java.nio.ByteBuffer
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags.MDB_NOMETASYNC
import org.lmdbjava.EnvFlags.MDB_NOSYNC
import org.lmdbjava.EnvFlags.MDB_NOTLS
import org.openjdk.jmh.annotations._
import com.google.common.io.Files
import com.graphula.Graphula

@State(Scope.Benchmark)
class LubmGraphula {

  implicit var sparql: Sparql = _

  def env(
    tmp: File,
    maxSizeInMegabytes: Long = 10000,
    durability: Boolean = false
  ): Env[ByteBuffer] = {
    val conf = Env.create()
      .setMapSize(maxSizeInMegabytes * 1000000)
      .setMaxDbs(3)
      .setMaxReaders(32767) // Maximum number of threads in a FJ pool.
    val flags = MDB_NOTLS :: { if (durability) Nil else MDB_NOSYNC :: MDB_NOMETASYNC :: Nil }
    conf.open(tmp, flags: _*)
  }

  @Setup
  def prepare: Unit = {
    val graphula = new Graphula(env(Files.createTempDir()))
    sparql = new Sparql(graphula)
    batchLoad(sparql)
  }

  @TearDown
  def check: Unit = {
    sparql.close
  }

  @Benchmark //  @Fork(jvmArgsAppend = Array("-agentpath:/Applications/YourKit-Java-Profiler-2016.02.app/Contents/Resources/bin/mac/libyjpagent.jnilib"))
  def q1: Int = {
    val q = queries(0)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q2: Int = {
    val q = queries(1)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q3: Int = {
    val q = queries(2)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q4: Int = {
    val q = queries(3)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q5: Int = {
    val q = queries(4)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q6: Int = {
    val q = queries(5)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q7: Int = {
    val q = queries(6)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q8: Int = {
    val q = queries(7)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q9: Int = {
    val q = queries(8)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark //  @Fork(jvmArgsAppend = Array("-agentpath:/Applications/YourKit-Java-Profiler-2016.02.app/Contents/Resources/bin/mac/libyjpagent.jnilib"))
  def q10: Int = {
    val q = queries(9)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q11: Int = {
    val q = queries(10)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q12: Int = {
    val q = queries(11)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q13: Int = {
    val q = queries(12)
    sparql.executeJenaQuery(q).size
  }

  @Benchmark
  def q14: Int = {
    val q = queries(13)
    sparql.executeJenaQuery(q).size
  }

}
