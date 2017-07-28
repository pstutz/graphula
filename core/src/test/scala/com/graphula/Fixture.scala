package com.graphula

import java.io.File
import java.nio.ByteBuffer

import org.lmdbjava.Cursor
import org.lmdbjava.Env
import org.lmdbjava.Txn
import org.scalatest.fixture.NoArg

import com.google.common.io.Files
import com.graphula.dictionary.Dictionary
import com.graphula.index.Index

import net.jpountz.xxhash.XXHash64
import net.jpountz.xxhash.XXHashFactory

object Fixture {

  def graphula(): (Graphula, Env[ByteBuffer], File) = {
    fixtureWithEnv(new Graphula(_))
  }

  def dictionary(hasher: XXHash64 = terribleHasher()): (Dictionary, Env[ByteBuffer], File) = {
    fixtureWithEnv(new Dictionary(_, hasher))
  }

  def index(): (Index, Env[ByteBuffer], File) = {
    fixtureWithEnv(new Index(_))
  }

  private def fixtureWithEnv[G](
    fixtureInit: Env[ByteBuffer] => G
  ): (G, Env[ByteBuffer], File) = {
    val tmp = Files.createTempDir()
    val env = Graphula.tempEnv()
    val fixture = fixtureInit(env)
    (fixture, env, tmp)
  }

  def withGraphula[G](block: Graphula => G): G = {
    withFixture(block)(graphula)
  }

  def withDictionary[G](block: Dictionary => G): G = {
    withFixture(block)(dictionary(terribleHasher()))
  }

  def withIndex[G](block: Index => G): G = {
    withFixture(block)(index)
  }

  private def withFixture[F, G](block: F => G)(
    init: => (F, Env[ByteBuffer], File)
  ): G = {
    val (fixture, env, file) = init
    try {
      block(fixture)
    } finally {
      env.close()
      file.delete()
    }
  }

  /**
   * A hasher that produces a lot of collisions to test exception paths.
   */
  private def terribleHasher(mod: Long = 10) = new XXHash64 {
    val actual = XXHashFactory.fastestInstance().hash64()
    override def hash(buf: Array[Byte], off: Int, len: Int, seed: Long): Long = {
      actual.hash(buf, off, len, seed) % mod
    }
    override def hash(buf: ByteBuffer, off: Int, len: Int, seed: Long): Long = {
      actual.hash(buf, off, len, seed) % mod
    }
  }

}

/**
 * Limited code-sharing due to initialization magic of NoArg.
 */
abstract class Fixture[G](initializer: => (G, Env[ByteBuffer], File))
    extends TransactionDelegator {
  protected implicit lazy val (fixture, env, tmp) = initializer
  protected def impl: Transaction
}

class GraphulaFixture(initializer: => (Graphula, Env[ByteBuffer], File))
    extends Fixture(initializer)
    with NoArg {
  def this() = this(Fixture.graphula())
  def graphula: Graphula = fixture
  protected def impl = graphula.transaction
  implicit val dictionary = graphula.dictionary
  override def apply(): Unit = {
    try {
      super.apply()
    } finally {
      env.close()
      tmp.delete()
    }
  }
}

class IndexFixture(initializer: => (Index, Env[ByteBuffer], File))
    extends Fixture(initializer)
    with NoArg {
  protected def impl = index.transaction
  def this() = this(Fixture.index())
  def index: Index = fixture
  override def apply(): Unit = {
    try {
      super.apply()
    } finally {
      env.close()
      tmp.delete()
    }
  }
}

class DictionaryFixture(initializer: => (Dictionary, Env[ByteBuffer], File))
    extends Fixture(initializer)
    with NoArg {
  protected def impl = dictionary.id2StringTransaction
  def this() = this(Fixture.dictionary())
  def dictionary: Dictionary = fixture
  override def apply(): Unit = {
    try {
      super.apply()
    } finally {
      env.close()
      tmp.delete()
    }
  }
}

trait TransactionDelegator {

  protected def impl: Transaction
  def read(): Txn[ByteBuffer] = impl.read()
  def releaseRead(implicit txn: Txn[ByteBuffer]): Unit = impl.releaseRead(txn)
  def withRead[G](block: Txn[ByteBuffer] => G): G = impl.withRead(block)
  def write(): Txn[ByteBuffer] = impl.write()
  def releaseWrite(implicit txn: Txn[ByteBuffer]): Unit =
    impl.releaseWrite(txn)
  def withWrite[G](block: Txn[ByteBuffer] => G): G = impl.withWrite(block)
  def withWriteCursor[G](block: Cursor[ByteBuffer] => G)(
    implicit
    txn: Txn[ByteBuffer]
  ): G = impl.withWriteCursor(block)
  def withCursor[G](block: Cursor[ByteBuffer] => G)(
    implicit
    txn: Txn[ByteBuffer]
  ): G = impl.withCursor(block)
  def close(): Unit = impl.close()

}
