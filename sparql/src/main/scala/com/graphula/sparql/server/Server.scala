package com.graphula.sparql.server

import com.graphula.Graphula
import com.graphula.sparql.Sparql
import java.io.File

object Server extends App {
  val env = Graphula.tempEnv()
  val g = new Graphula(env)
  val s = new Sparql(g)
  s.loadNtriples(new File("wikidata-simple-statements.nt"))

}
