package com.graphula.dictionary

import org.scalatest.fixture.FlatSpec
import org.scalatest.fixture.NoArg
import org.scalatest.fixture.UnitFixture
import org.scalatest.prop.Checkers

import com.graphula.DictionaryFixture
import com.graphula.Fixture.withDictionary

class DictionarySpec extends FlatSpec with Checkers with UnitFixture {

  /**
   * The empty string needs to own its hash code in the Long -> String map,
   * because it is not a legal key for the String -> Long exception map.
   */
  "Dictionary" should "already contain the empty string with its own hash code" in new DictionaryFixture {
    val empty = ""
    val id = withWrite { implicit txn =>
      dictionary.add(empty)
    }
    assert(id == dictionary.hash(empty))
    withRead { implicit txn =>
      val decoded = dictionary(id)
      assert(decoded == empty)
    }
  }

  it should "encode and decode strings" in new NoArg {
    check(
      (strings: List[String]) => {
        withDictionary { implicit dictionary =>
          import dictionary.id2StringTransaction._
          val ids = withWrite { implicit txn =>
            strings.map(dictionary.add(_))
          }
          withRead { implicit txn =>
            val decoded = ids.map(dictionary(_))
            assert(decoded == strings)
          }
        }
        true
      },
      minSuccessful(5)
    )
  }

  it should "support adding entries concurrently" in new DictionaryFixture {
    val strings = (1 to 1000).map(_.toString)
    // Access dictionary to trigger initialization, this gets stuck when done by parallel collections.
    dictionary
    val ids = strings.par.map { string =>
      withWrite { implicit txn =>
        dictionary.add(string)
      }
    }.toSet
    withRead { implicit txn =>
      val decoded = ids.map(dictionary(_))
      assert(decoded.size == 1000)
      assert(decoded.toSet == strings.toSet)
    }
  }

  it should "support parallel operations with blank nodes" in new NoArg {
    check(
      (ids: Set[Int]) => {
        withDictionary {
          dictionary =>
            import dictionary.id2StringTransaction._
            withRead {
              implicit txn =>
                for { id <- ids } {
                  assert(!dictionary.isBlankNode(id))
                }
                val bnIds =
                  (1 to 100).map(_ => dictionary.getBlankNode()).toSeq.toSet
                for { id <- ids } {
                  assert(dictionary.isBlankNode(id) == bnIds.contains(id))
                }
                for { bn <- bnIds } {
                  assert(dictionary(bn) == s"_:$bn")
                }
            }
        }
        true
      },
      minSuccessful(50)
    )
  }

}
