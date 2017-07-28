package com.graphula.sparql

import org.scalatest.fixture.FlatSpec
import org.scalatest.fixture.UnitFixture
import org.scalatest.prop.Checkers
import org.apache.jena.graph.NodeFactory
import org.apache.jena.graph.Triple
import org.apache.jena.query.QueryFactory
import org.apache.jena.query.QueryExecutionFactory
import collection.JavaConversions._
import com.graphula.SparqlFixture

class SparqlSpec extends FlatSpec with Checkers with UnitFixture {

  "Sparql" should "answer a basic graph pattern" in new SparqlFixture {
    val prefix = "http://"
    val s = NodeFactory.createURI(s"${prefix}s")
    val p = NodeFactory.createURI(s"${prefix}p")
    val o = NodeFactory.createURI(s"${prefix}o")
    sparql.add(new Triple(s, p, o))
    sparql.add(new Triple(o, p, o))
    val queryString = """ | PREFIX p: <http://>
                | SELECT ?X ?Y
                | WHERE {
                |  ?X p:p ?Y .
                |  ?Y p:p p:o .
                |}""".stripMargin
    val results = sparql.execute(queryString)
    val resultMaps = results.map { r =>
      r.varNames.map { variable =>
        variable -> r.get(variable).toString
      }.toMap
    }.toSet
    assert(
      resultMaps == Set(
        Map("X" -> s"${prefix}s", "Y" -> s"${prefix}o"),
        Map("X" -> s"${prefix}o", "Y" -> s"${prefix}o")
      )
    )
  }

  it should "answer a graph pattern that contains a failing existence check" in new SparqlFixture {
    val prefix = "http://"
    val s = NodeFactory.createURI(s"${prefix}s")
    val p = NodeFactory.createURI(s"${prefix}p")
    val o = NodeFactory.createURI(s"${prefix}o")
    sparql.add(new Triple(s, p, o))
    val queryString = """ | PREFIX p: <http://>
                | SELECT ?X ?Y
                | WHERE {
                |  ?X p:p ?Y .
                |  ?Y p:p p:o .
                |}""".stripMargin
    val results = sparql.execute(queryString)
    val resultMaps = results.map { r =>
      r.varNames.map { variable =>
        variable -> r.get(variable).toString
      }.toMap
    }.toSet
    assert(resultMaps == Set.empty)
  }

}
