package com.graphula.index

import java.nio.LongBuffer

import org.scalatest.fixture.FlatSpec
import org.scalatest.fixture.NoArg
import org.scalatest.fixture.UnitFixture
import org.scalatest.prop.Checkers

import com.graphula.Fixture.withIndex
import com.graphula.Generators.arbitraryTriple
import com.graphula.Graphula.unbound
import com.graphula.IndexFixture
import com.graphula.TriplePattern

class IndexSpec extends FlatSpec with Checkers with UnitFixture {

  "Index" should "put and get values" in new NoArg {
    check(
      (key: TriplePattern, values: Set[Long]) => {
        withIndex {
          implicit index =>
            import index.transaction._
            withRead { implicit txn =>
              withCursor { implicit c =>
                assert(!index.exists(key.s, key.p, key.o))
              }
            }
            withWrite { implicit txn =>
              withWriteCursor { implicit c =>
                for { value <- values } {
                  index.put(key.s, key.p, key.o, value)
                }
              }
            }
            withRead { implicit txn =>
              withCursor { implicit c =>
                assert(values.isEmpty || index.exists(key.s, key.p, key.o))
                val retrievedValues =
                  bufferToLongs(index.values(key.s, key.p, key.o)).toSet
                assert(retrievedValues == values)
              }
            }
        }
        true
      },
      minSuccessful(3)
    )
  }

  it should "support entries that take up more than 511 bytes" in new IndexFixture {
    val maxId = 50000
    val ids = Set(1 to maxId: _*)
    withWrite { implicit txn =>
      withWriteCursor { implicit c =>
        ids.foreach(id => index.put(unbound, unbound, unbound, id))
      }
    }
    withRead { implicit txn =>
      withCursor { implicit c =>
        assert(ids == bufferToLongs(index.values(0, 0, 0)).toSet)
      }
    }
  }

  it should "build index entries from triples" in new NoArg {
    check(
      (patterns: List[TriplePattern]) => {
        withIndex {
          implicit index =>
            import index.transaction._
            val patternSet = patterns.toSet
            withWrite { implicit txn =>
              withWriteCursor { implicit c =>
                for { p <- patterns } {
                  index.addTriple(p.s, p.p, p.o)
                }
              }
            }
            withRead {
              implicit txn =>
                withCursor {
                  implicit c =>
                    val rootDeltas = patternSet.map(_.p)
                    assert(rootDeltas.isEmpty || index.exists(0, 0, 0))
                    val retrievedRootDeltas =
                      bufferToLongs(index.values(0, 0, 0)).toSet
                    assert(retrievedRootDeltas == rootDeltas)
                    val predicateDeltas = patternSet.groupBy(_.p).map {
                      case (p, ps) => (p, ps.map(_.s))
                    }
                    for { (p, deltas) <- predicateDeltas } {
                      val pPattern = TriplePattern(unbound, p, unbound)
                      assert(index.exists(pPattern.s, pPattern.p, pPattern.o))
                      val retrievedPredicateDeltas = bufferToLongs(
                        index
                          .values(pPattern.s, pPattern.p, pPattern.o)
                      ).toSet
                      assert(retrievedPredicateDeltas == deltas)
                    }
                    val subjectDeltas = patternSet.groupBy(_.s).map {
                      case (s, ps) => (s, ps.map(_.p))
                    }
                    for { (s, deltas) <- subjectDeltas } {
                      val sPattern = TriplePattern(s, unbound, unbound)
                      assert(index.exists(sPattern.s, sPattern.p, sPattern.o))
                      val retrievedSubjectDeltas = bufferToLongs(
                        index
                          .values(sPattern.s, sPattern.p, sPattern.o)
                      ).toSet
                      assert(retrievedSubjectDeltas == deltas)
                    }
                    val objectDeltas = patternSet.groupBy(_.o).map {
                      case (o, ps) => (o, ps.map(_.p))
                    }
                    for { (o, deltas) <- objectDeltas } {
                      val oPattern = TriplePattern(unbound, unbound, o)
                      assert(index.exists(oPattern.s, oPattern.p, oPattern.o))
                      val retrievedObjectDeltas = bufferToLongs(
                        index
                          .values(oPattern.s, oPattern.p, oPattern.o)
                      ).toSet
                      assert(retrievedObjectDeltas == deltas)
                    }
                    val spDeltas = patternSet.groupBy(p => (p.s, p.p)).map {
                      case (tuple, ps) => (tuple, ps.map(_.o))
                    }
                    for { ((s, p), deltas) <- spDeltas } {
                      val spPattern = TriplePattern(s, p, unbound)
                      assert(
                        index.exists(spPattern.s, spPattern.p, spPattern.o)
                      )
                      val retrievedSpDeltas =
                        bufferToLongs(
                          index.values(
                            spPattern.s,
                            spPattern.p,
                            spPattern.o
                          )
                        ).toSet
                      assert(retrievedSpDeltas == deltas)
                    }
                    val soDeltas = patternSet.groupBy(p => (p.s, p.o)).map {
                      case (tuple, ps) => (tuple, ps.map(_.p))
                    }
                    for { ((s, o), deltas) <- soDeltas } {
                      val soPattern = TriplePattern(s, unbound, o)
                      assert(
                        index.exists(soPattern.s, soPattern.p, soPattern.o)
                      )
                      val retrievedSoDeltas =
                        bufferToLongs(
                          index.values(
                            soPattern.s,
                            soPattern.p,
                            soPattern.o
                          )
                        ).toSet
                      assert(retrievedSoDeltas == deltas)
                    }
                    val poDeltas = patternSet.groupBy(p => (p.p, p.o)).map {
                      case (tuple, ps) => (tuple, ps.map(_.s))
                    }
                    for { ((p, o), deltas) <- poDeltas } {
                      val poPattern = TriplePattern(unbound, p, o)
                      assert(
                        index.exists(poPattern.s, poPattern.p, poPattern.o)
                      )
                      val retrievedPoDeltas =
                        bufferToLongs(
                          index.values(
                            poPattern.s,
                            poPattern.p,
                            poPattern.o
                          )
                        ).toSet
                      assert(retrievedPoDeltas == deltas)
                    }
                    for { p <- patternSet } {
                      assert(index.exists(p.s, p.p, p.o))
                      val exists =
                        bufferToLongs(index.values(p.s, p.p, p.o)).toSet
                      assert(exists == Set(Index.existenceMarker))
                    }
                }
            }
        }
        true
      },
      minSuccessful(20)
    )
  }

  private def bufferToLongs(b: LongBuffer): List[Long] = {
    var valuesList = List.empty[Long]
    while (b.hasRemaining()) {
      valuesList = b.get() :: valuesList
    }
    valuesList.reverse
  }

}
