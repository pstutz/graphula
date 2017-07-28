package com.graphula.benchmarks.lubm

import org.apache.jena.riot.RDFDataMgr
import java.io.FileInputStream
import org.apache.jena.rdf.model.Model
import java.io.File
import org.apache.jena.riot.Lang._
import org.apache.jena.query._
import com.graphula.sparql.Sparql

object Lubm {

  def batchLoad(sparql: Sparql, toId: Int = 14): Unit = {
    println(s"loading ${toId + 1} lubm files")
    for (fileNumber <- 0 to toId) {
      print(s"loading file $fileNumber ... ")
      sparql.loadNtriples(new File(s"./data/university0_${fileNumber}.nt"))
      println("done")
    }
    println("all files loaded")
  }

  def load(m: Model, toId: Int = 14): Unit = {
    println(s"loading ${toId + 1} lubm files")
    for (fileNumber <- 0 to toId) {
      print(s"loading file $fileNumber ... ")
      val tripleStream = new FileInputStream(new File(s"./data/university0_${fileNumber}.nt"))
      RDFDataMgr.read(m, tripleStream, NTRIPLES)
      println("done")
    }
  }

  val queries = Array(
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> .
  ?X rdf:type ub:GraduateStudent .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?Z rdf:type ub:Department .
  ?Z ub:subOrganizationOf ?Y .
  ?Y rdf:type ub:University .
  ?X ub:undergraduateDegreeFrom ?Y .
  ?X ub:memberOf ?Z .
  ?X rdf:type ub:GraduateStudent .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:publicationAuthor <http://www.Department0.University0.edu/AssistantProfessor0> .
  ?X rdf:type ub:Publication .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y1 ?Y2 ?Y3
WHERE {
  ?X ub:worksFor <http://www.Department0.University0.edu> .
        ?X rdf:type ub:Professor .
  ?X ub:name ?Y1 .
  ?X ub:emailAddress ?Y2 .
  ?X ub:telephone ?Y3 .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:memberOf <http://www.Department0.University0.edu> .
  ?X rdf:type ub:Person .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X WHERE {?X rdf:type ub:Student}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y
WHERE {
  <http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y .
  ?Y rdf:type ub:Course .
  ?X ub:takesCourse ?Y .
  ?X rdf:type ub:Student .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?Y ub:subOrganizationOf <http://www.University0.edu> .
  ?Y rdf:type ub:Department .
  ?X ub:memberOf ?Y .
  ?X rdf:type ub:Student .
  ?X ub:emailAddress ?Z .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?Y rdf:type ub:Faculty .
  ?Y ub:teacherOf ?Z .
  ?X ub:advisor ?Y .
  ?X ub:takesCourse ?Z .
  ?Z rdf:type ub:Course .
  ?X rdf:type ub:Student .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> .
  ?X rdf:type ub:Student .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:subOrganizationOf <http://www.University0.edu> .
  ?X rdf:type ub:ResearchGroup .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y
WHERE {
  ?Y ub:subOrganizationOf <http://www.University0.edu> .
  ?Y rdf:type ub:Department .
  ?X ub:worksFor ?Y .
  ?X rdf:type ub:Chair .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X WHERE {
  <http://www.University0.edu> ub:hasAlumnus ?X .
  ?X rdf:type ub:Person .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X rdf:type ub:UndergraduateStudent .
}
    """
  ).map(QueryFactory.create(_))

}
