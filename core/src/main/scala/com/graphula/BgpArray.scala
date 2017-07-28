package com.graphula

import java.nio.LongBuffer

import scala.annotation.tailrec
import scala.language.implicitConversions

final object BgpArray {

  val cardinalityOffset = 0
  val subjectOffset = 1
  val predicateOffset = 2
  val objectOffset = 3
  val tupleSize = 4
  val emptyArrayOfBgps = new Array[Array[Long]](0)

  implicit def triplePatternsToLongArray(
    ps: List[TriplePattern]
  ): Array[Long] = {
    val patterns = new Array[Long](ps.size * tupleSize)
    @tailrec
    def recInitializeTriplePattern(ps: List[TriplePattern], index: Int): Unit = {
      ps match {
        case Nil =>
        case h :: t =>
          patterns(index + cardinalityOffset) = Long.MaxValue
          patterns(index + subjectOffset) = h.s
          patterns(index + predicateOffset) = h.p
          patterns(index + objectOffset) = h.o
          recInitializeTriplePattern(t, index + tupleSize)
      }
    }
    recInitializeTriplePattern(ps, 0)
    patterns
  }

  implicit final class BgpArrayWrapper(val bgpArray: Array[Long])
      extends AnyVal {

    def asTriplePatternList: List[TriplePattern] = {
      bgpArray.grouped(tupleSize).toList.map { t =>
        TriplePattern(t(subjectOffset), t(predicateOffset), t(objectOffset))
      }
    }

    def variableOccurrences: Array[Int] = {
      var requiredArrayEntries = 0
      var i = 0
      while (i < bgpArray.length) {
        val variableId = bgpArray(i)
        if (variableId < 0) {
          val index = BindingArray.indexForVariableId(variableId.toInt)
          requiredArrayEntries = math.max(requiredArrayEntries, index + 1)
        }
        i += 1
      }
      i = 0
      val o = new Array[Int](requiredArrayEntries)
      while (i < bgpArray.length) {
        val variableId = bgpArray(i)
        if (variableId < 0) {
          val index = BindingArray.indexForVariableId(variableId.toInt)
          o(index) = o(index) + 1
        }
        i += 1
      }
      o
    }

    def variableCoOccurrences: Array[Int] = {
      var requiredArrayEntries = 0
      var i = 0
      while (i < bgpArray.length) {
        val variableId = bgpArray(i)
        if (variableId < 0) {
          val index = BindingArray.indexForVariableId(variableId.toInt)
          requiredArrayEntries = math.max(requiredArrayEntries, index + 1)
        }
        i += 1
      }
      variableCoOccurrencesWithSize(requiredArrayEntries)
    }

    def variableCoOccurrencesWithSize(size: Int): Array[Int] = {
      var i = 0
      val r = new Array[Int](size)
      while (i < bgpArray.length) {
        val s = bgpArray(i + subjectOffset)
        val p = bgpArray(i + predicateOffset)
        val o = bgpArray(i + objectOffset)
        if ((s < 0 && p < 0) || (s < 0 && o < 0) || (p < 0 && o < 0)) {
          if (s < 0) {
            val sIndex = BindingArray.indexForVariableId(s.toInt)
            r(sIndex) = r(sIndex) + 1
          }
          if (p < 0) {
            val pIndex = BindingArray.indexForVariableId(p.toInt)
            r(pIndex) = r(pIndex) + 1
          }
          if (o < 0) {
            val oIndex = BindingArray.indexForVariableId(o.toInt)
            r(oIndex) = r(oIndex) + 1
          }
        }
        i += tupleSize
      }
      r
    }

    @inline def unboundPositions: Int = {
      var unbound = 0
      var i = 0
      while (i < bgpArray.length) {
        val s = bgpArray(i + subjectOffset)
        val p = bgpArray(i + predicateOffset)
        val o = bgpArray(i + objectOffset)
        unbound += (s >>> 63).toInt
        unbound += (p >>> 63).toInt
        unbound += (o >>> 63).toInt
        i += tupleSize
      }
      unbound
    }

    @inline def isResult: Boolean = bgpArray.length == 0

    @inline def headCardinality: Long = bgpArray(cardinalityOffset)
    @inline def headSubject: Long = bgpArray(subjectOffset)
    @inline def headPredicate: Long = bgpArray(predicateOffset)
    @inline def headObject: Long = bgpArray(objectOffset)

    /**
     * Swaps the triple patterns that start at indexes `a` and `b`.
     */
    @inline def swap(a: Int, b: Int): Unit = {
      val bCardinalityIndex = b + cardinalityOffset
      val bSubjectIndex = b + subjectOffset
      val bPredicateIndex = b + predicateOffset
      val bObjectIndex = b + objectOffset

      val aCardinalityIndex = a + cardinalityOffset
      val aSubjectIndex = a + subjectOffset
      val aPredicateIndex = a + predicateOffset
      val aObjectIndex = a + objectOffset

      val bCardinalityBackup = bgpArray(bCardinalityIndex)
      val bSubjectBackup = bgpArray(bSubjectIndex)
      val bPredicateBackup = bgpArray(bPredicateIndex)
      val bObjectBackup = bgpArray(bObjectIndex)

      bgpArray(bCardinalityIndex) = bgpArray(aCardinalityIndex)
      bgpArray(bSubjectIndex) = bgpArray(aSubjectIndex)
      bgpArray(bPredicateIndex) = bgpArray(aPredicateIndex)
      bgpArray(bObjectIndex) = bgpArray(aObjectIndex)

      //      println(s"swapping [$bSubjectBackup $bPredicateBackup $bObjectBackup] with [${bgpArray(aSubjectIndex)} ${bgpArray(aPredicateIndex)} ${bgpArray(aObjectIndex)}]")

      bgpArray(aCardinalityIndex) = bCardinalityBackup
      bgpArray(aSubjectIndex) = bSubjectBackup
      bgpArray(aPredicateIndex) = bPredicateBackup
      bgpArray(aObjectIndex) = bObjectBackup
    }

    @inline def isHeadFullyBound: Boolean =
      headSubject > 0 && headPredicate > 0 && headObject > 0

    @inline def bgpTail: Array[Long] = {
      if (bgpArray.length > tupleSize) {
        val copy = new Array[Long](bgpArray.length - tupleSize)
        System.arraycopy(bgpArray, tupleSize, copy, 0, copy.length)
        copy
      } else {
        Array.emptyLongArray
      }
    }

    @inline def bindVariable(variable: Long, value: Long): Array[Long] = {
      val s = headSubject
      val p = headPredicate
      val o = headObject
      val toBind =
        if ((s > 0 || s == variable) && (p > 0 || p == variable) && (o > 0 || o == variable)) {
          bgpTail
        } else {
          bgpArray.clone
        }
      var i = 0
      while (i < toBind.length) {
        val sIndex = i + subjectOffset
        val s = toBind(sIndex)
        if (s == variable) {
          toBind(sIndex) = value
          toBind(i + cardinalityOffset) = Long.MaxValue
        }
        val pIndex = i + predicateOffset
        val p = toBind(pIndex)
        if (p == variable) {
          toBind(pIndex) = value
          toBind(i + cardinalityOffset) = Long.MaxValue
        }
        val oIndex = i + objectOffset
        val o = toBind(oIndex)
        if (o == variable) {
          toBind(oIndex) = value
          toBind(i + cardinalityOffset) = Long.MaxValue
        }
        i += tupleSize
      }
      toBind
    }

    @inline
    def bindMany(variable: Long, bindings: LongBuffer): Array[Array[Long]] = {
      if (!bindings.hasRemaining) {
        emptyArrayOfBgps
      } else {
        val s = headSubject
        val p = headPredicate
        val o = headObject
        val toBind =
          if ((s > 0 || s == variable) && (p > 0 || p == variable) && (o > 0 || o == variable)) {
            bgpTail
          } else {
            bgpArray
          }
        val numberOfResultArrays = bindings.remaining
        val resultArrays = new Array[Array[Long]](numberOfResultArrays)
        var i = 0
        while (i < numberOfResultArrays) {
          resultArrays(i) = toBind.clone
          i += 1
        }
        i = 0
        while (i < toBind.length) {
          val sIndex = i + subjectOffset
          val s = toBind(sIndex)
          if (s == variable) {
            val cardinalityIndex = i + cardinalityOffset
            var j = 0
            while (j < numberOfResultArrays) {
              resultArrays(j)(sIndex) = bindings.get(j)
              resultArrays(j)(cardinalityIndex) = Long.MaxValue
              j += 1
            }
          }
          val pIndex = i + predicateOffset
          val p = toBind(pIndex)
          if (p == variable) {
            val cardinalityIndex = i + cardinalityOffset
            var j = 0
            while (j < numberOfResultArrays) {
              resultArrays(j)(pIndex) = bindings.get(j)
              resultArrays(j)(cardinalityIndex) = Long.MaxValue
              j += 1
            }
          }
          val oIndex = i + objectOffset
          val o = toBind(oIndex)
          if (o == variable) {
            val cardinalityIndex = i + cardinalityOffset
            var j = 0
            while (j < numberOfResultArrays) {
              resultArrays(j)(oIndex) = bindings.get(j)
              resultArrays(j)(cardinalityIndex) = Long.MaxValue
              j += 1
            }
          }
          i += tupleSize
        }
        resultArrays
      }
    }

  }

}
