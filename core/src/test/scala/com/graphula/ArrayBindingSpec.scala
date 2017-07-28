package com.graphula

import java.nio.LongBuffer

import org.scalatest.fixture.FlatSpec
import org.scalatest.fixture.UnitFixture

import com.graphula.BindingArray.BindingArrayWrapper
import com.graphula.BindingArray.unbound

class ArrayBindingSpec extends FlatSpec with UnitFixture {

  "Default binding" should "support adding and retrieving a binding" in new GraphulaFixture {
    val b = BindingArray.empty
    withRead { implicit txn =>
      val variable = -1
      val binding = 1L
      val updated = b.addBinding(variable, new LazyBinding(binding))
      assert(updated.getUnboundAsZero(-1) == binding)
      assert(updated.getUnboundAsZero(-2) == unbound)
    }
  }

  it should "support extension with a LongBuffer of binding values" in new GraphulaFixture {
    val b = BindingArray.empty
    val bindings = List(1L, 2L, 3L)
    val buffer = LongBuffer.wrap(bindings.toArray)
    withRead { implicit txn =>
      val variable = -1
      val updatedList = b.addBindings(variable, buffer).toList
      for { i <- 0 until bindings.size } {
        assert(updatedList(i).getUnboundAsZero(-1) == bindings(i))
        assert(updatedList(i).getUnboundAsZero(-2) == unbound)
      }
    }
  }

}
