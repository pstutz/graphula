package com.graphula

import org.scalacheck.Arbitrary
import org.scalacheck.Gen

object Generators {

  val maxId = Long.MaxValue
  val generateTriple = for {
    s <- Gen.choose(1, maxId)
    p <- Gen.choose(1, maxId)
    o <- Gen.choose(1, maxId)
  } yield TriplePattern(s, p, o)
  implicit val arbitraryTriple = Arbitrary(generateTriple)

}
