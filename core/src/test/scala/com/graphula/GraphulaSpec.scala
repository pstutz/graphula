package com.graphula

import org.scalatest.fixture.FlatSpec
import org.scalatest.fixture.UnitFixture

import com.graphula.BgpArray.triplePatternsToLongArray
import com.graphula.BindingArray.BindingArrayWrapper

class GraphulaSpec extends FlatSpec with UnitFixture {

  "Graphula" should "support querying for a basic graph pattern" in new GraphulaFixture {
    withWrite { implicit txn =>
      withWriteCursor { implicit c =>
        graphula.addToIndex(1, 2, 3)
        graphula.addToIndex(3, 4, 5)
        graphula.addToIndex(3, 4, 6)
        graphula.addToIndex(5, 2, 5)
        graphula.addToIndex(6, 2, 5)
      }
    }
    withRead { implicit txn =>
      withCursor { implicit c =>
        val results = graphula.execute(
          List(
            TriplePattern(-1, 2, -2),
            TriplePattern(-2, 4, -3),
            TriplePattern(-3, 2, 5)
          )
        )
        val listOfBindingLists = results
          .map(
            b =>
              List(
                b.getUnboundAsZero(-1),
                b.getUnboundAsZero(-2),
                b.getUnboundAsZero(-3)
              )
          )
          .toSet
        assert(listOfBindingLists == Set(List(1, 3, 5), List(1, 3, 6)))
      }
    }
  }

  it should "handle a query with no parallelism" in new GraphulaFixture {
    withWrite { implicit txn =>
      withWriteCursor { implicit c =>
        graphula.addToIndex(1, 2, 3)
        graphula.addToIndex(3, 4, 5)
        graphula.addToIndex(3, 4, 6)
        graphula.addToIndex(3, 5, 7)
        graphula.addToIndex(5, 2, 5)
        graphula.addToIndex(6, 2, 5)
      }
    }
    withRead { implicit txn =>
      withCursor { implicit c =>
        val results = graphula.execute(
          List(
            TriplePattern(-1, 4, 5),
            TriplePattern(-1, 4, 6),
            TriplePattern(-1, 5, 7)
          )
        )
        val setOfBindingLists =
          results.map(b => List(b.getUnboundAsZero(-1))).toSet
        assert(setOfBindingLists == Set(List(3)))
      }
    }
  }

  it should "answer a simple pattern with many solutions" in new GraphulaFixture {
    val maxS = 2L
    val maxP = 2L
    val maxO = 2L
    def load(graphula: Graphula): Unit = {
      withWrite { implicit txn =>
        withWriteCursor { implicit c =>
          for {
            s <- 1L to maxS
            p <- 1L to maxP
            o <- 1L to maxO
          } graphula.addToIndex(s, p, o)
        }
      }
    }
    load(graphula)
    withRead { implicit txn =>
      withCursor { implicit c =>
        val results = graphula.execute(
          List(TriplePattern(-1, 1, -2), TriplePattern(-2, 2, -3))
        )
        val setOfBindingLists = results
          .map(
            b =>
              List(
                b.getUnboundAsZero(-1),
                b.getUnboundAsZero(-2),
                b.getUnboundAsZero(-3)
              )
          )
          .toSet
        assert(
          setOfBindingLists == Set(
            List(2, 1, 2),
            List(2, 2, 2),
            List(2, 2, 1),
            List(1, 1, 2),
            List(1, 1, 1),
            List(1, 2, 1),
            List(2, 1, 1),
            List(1, 2, 2)
          )
        )
      }
    }
  }

}
