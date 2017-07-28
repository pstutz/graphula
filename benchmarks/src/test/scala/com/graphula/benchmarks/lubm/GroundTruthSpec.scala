package com.graphula.benchmarks.lubm

import java.io.File

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaIterator
import scala.collection.immutable.TreeMap
import scala.io.Source

import java.nio.ByteBuffer
import org.apache.jena.query.Query
import org.apache.jena.query.QueryExecutionFactory
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags.MDB_NOMETASYNC
import org.lmdbjava.EnvFlags.MDB_NOSYNC
import org.lmdbjava.EnvFlags.MDB_NOTLS
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.google.common.io.Files
import com.graphula.Graphula
import com.graphula.sparql.Sparql

class GroundTruthSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val enabledQueries = Set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)

  "LUBM Query 1" should "match the reference results" in {
    runTest(1)
  }

  "LUBM Query 2" should "match the reference results" in {
    runTest(2)
  }

  "LUBM Query 3" should "match the reference results" in {
    runTest(3)
  }

  "LUBM Query 4" should "match the reference results" in {
    runTest(4)
  }

  "LUBM Query 5" should "match the reference results" in {
    runTest(5)
  }

  "LUBM Query 6" should "match the reference results" in {
    runTest(6)
  }

  "LUBM Query 7" should "match the reference results" in {
    runTest(7)
  }

  "LUBM Query 8" should "match the reference results" in {
    runTest(8)
  }

  "LUBM Query 9" should "match the reference results" in {
    runTest(9)
  }

  "LUBM Query 10" should "match the reference results" in {
    runTest(10)
  }

  "LUBM Query 11" should "match the reference results" in {
    runTest(11)
  }

  "LUBM Query 12" should "match the reference results" in {
    runTest(12)
  }

  "LUBM Query 13" should "match the reference results" in {
    runTest(13)
  }

  "LUBM Query 14" should "match the reference results" in {
    runTest(14)
  }

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

  val graphula = new Graphula(env(Files.createTempDir()))
  val sparql = new Sparql(graphula)
  val model = sparql.model

  override def beforeAll {
    Lubm.batchLoad(sparql)
  }

  override def afterAll {
    model.close
  }

  def executeOnQueryEngine(q: Query): List[Bindings] = {
    val bindings = sparql.executeJenaQuery(q)
    val bindingsList = bindings.map { binding =>
      val bindingsMap = bindings.getResultVars.map(
        variable => (variable, binding.get(variable).toString)
      ).toMap
      bindingsMap
    }.toList
    val sortedBindings: List[TreeMap[String, String]] = bindingsList.
      map(unsortedBindings => TreeMap(unsortedBindings.toArray: _*))
    val sortedBindingList = (sortedBindings.sortBy(map => map.values)).toList
    sortedBindingList
  }

  type Bindings = TreeMap[String, String]
  type QuerySolution = List[Bindings]

  def runTest(queryId: Int) {
    if (enabledQueries.contains(queryId)) {
      val referenceResult = referenceResults(queryId)
      val ourResult = executeOnQueryEngine(Lubm.queries(queryId - 1))
      println(s"reference length = ${referenceResult.size}, ourResultLength=${ourResult.size}")
      assert(ourResult == referenceResult, s"Graphula result $ourResult for query $queryId did not match reference result $referenceResult.")
    }
  }

  val answerResources = (1 to 14).map { queryNumber =>
    queryNumber -> s"./data/answers_query$queryNumber.txt"
  }.toMap
  val referenceResults: Map[Int, QuerySolution] = {
    answerResources map { entry =>
      val resource = entry._2
      val source = Source.fromFile(resource)
      val lines = source.getLines
      val bindings = getQueryBindings(lines)
      (entry._1, bindings)
    }
  }

  def getQueryBindings(lines: Iterator[String]): QuerySolution = {
    var currentLine = lines.next
    if (currentLine == "NO ANSWERS.") {
      // No bindings.
      List()
    } else {
      val variables = currentLine.split("\t").toIndexedSeq
      var solution = List[Bindings]()
      while (lines.hasNext) {
        var binding = TreeMap[String, String]()
        currentLine = lines.next
        val values = currentLine.split("\t").toIndexedSeq
        for (i <- 0 until variables.size) {
          binding += variables(i) -> values(i)
        }
        solution = binding :: solution
      }
      solution.sortBy(map => map.values)
    }
  }

}
