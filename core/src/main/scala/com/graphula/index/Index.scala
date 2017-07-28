package com.graphula.index

import java.nio.ByteBuffer
import java.nio.LongBuffer

import scala.annotation.tailrec

import org.lmdbjava.Cursor
import org.lmdbjava.DbiFlags.MDB_CREATE
import org.lmdbjava.DbiFlags.MDB_DUPFIXED
import org.lmdbjava.DbiFlags.MDB_DUPSORT
import org.lmdbjava.DbiFlags.MDB_INTEGERDUP
import org.lmdbjava.Env
import org.lmdbjava.GetOp.MDB_SET
import org.lmdbjava.PutFlags.MDB_NODUPDATA
import org.lmdbjava.SeekOp._
import org.lmdbjava.Txn

import com.graphula.Graphula.unbound
import com.graphula.Transaction
import com.graphula.util.ThreadLocalSizedBuffer

final object Index {

  private val indexPatternSize = 24
  private val longSize = 8
  private val subjectOffset = 0
  private val predicateOffset = 8
  private val objectOffset = 16

  val emptyArray = Array.emptyLongArray
  val emptyBuffer: LongBuffer = LongBuffer.wrap(emptyArray)

  /**
   * Marker is irrelevant, as a key existing is enough for a triple to be considered
   * present.
   */
  val existenceMarker = 1

}

final class Index(env: Env[ByteBuffer]) {

  private val indexDb = env.openDbi(
    "IndexDb",
    MDB_CREATE,
    MDB_DUPSORT,
    MDB_DUPFIXED,
    MDB_INTEGERDUP
  )

  val transaction = new Transaction(env, indexDb)

  def close(): Unit = {
    transaction.close()
  }

  /**
   * Requires a write cursor.
   */
  @inline
  def addTriple(s: Long, p: Long, o: Long)(
    implicit
    c: Cursor[ByteBuffer]
  ): Unit = {
    if (put(s, p, o, Index.existenceMarker)) {
      put(s, unbound, o, p)
      if (put(unbound, p, o, s)) {
        put(unbound, unbound, o, p)
      }
      if (put(s, p, unbound, o)) {
        put(s, unbound, unbound, p)
        if (put(unbound, p, unbound, s)) {
          put(unbound, unbound, unbound, p)
        }
      }
    }
  }

  private val keyBuffer = new ThreadLocalSizedBuffer(Index.indexPatternSize)
  private val valueBuffer = new ThreadLocalSizedBuffer(Index.longSize)

  @inline private def indexKey(s: Long, p: Long, o: Long): ByteBuffer = {
    val key = keyBuffer.get
    key.putLong(Index.subjectOffset, s)
    key.putLong(Index.predicateOffset, p)
    key.putLong(Index.objectOffset, o)
    key
  }

  @inline private def indexValue(v: Long): ByteBuffer = {
    val value = valueBuffer.get
    value.putLong(0, v)
    value
  }

  /**
   * Adds value `v` for index pattern SPO. Returns true iff the value didn't exist already.
   * Requires a write cursor.
   */
  @inline
  def put(s: Long, p: Long, o: Long, v: Long)(
    implicit
    c: Cursor[ByteBuffer]
  ): Boolean = {
    c.put(indexKey(s, p, o), indexValue(v), MDB_NODUPDATA)
  }

  /**
   * Returns true iff the SPO pattern exists as a key in the store.
   */
  @inline
  def exists(s: Long, p: Long, o: Long)(
    implicit
    c: Cursor[ByteBuffer]
  ): Boolean = {
    c.get(indexKey(s, p, o), MDB_SET)
  }

  @inline
  def valueCount(s: Long, p: Long, o: Long)(
    implicit
    c: Cursor[ByteBuffer]
  ): Long = {
    val keyExists = c.get(indexKey(s, p, o), MDB_SET)
    if (keyExists) {
      c.count
    } else {
      0L
    }
  }

  /**
   * Returns a buffer with all values for index key pattern SPO. Returns true iff the value didn't exist already.
   * Requires a cursor tied to a read transaction.
   */
  @inline
  def values(s: Long, p: Long, o: Long)(
    implicit
    c: Cursor[ByteBuffer]
  ): LongBuffer = {
    val keyExists = c.get(indexKey(s, p, o), MDB_SET)
    if (keyExists) {
      c.seek(MDB_GET_MULTIPLE)
      val count = c.count
      val values = c.`val`.asLongBuffer
      if (count == values.remaining) {
        values
      } else {
        val results = new Array[Long](count.toInt)
        @inline
        @tailrec def recursiveRead(values: LongBuffer, offset: Int): Unit = {
          val longs = values.remaining
          values.get(results, offset, longs)
          if (c.seek(MDB_NEXT_MULTIPLE)) {
            val nextValues = c.`val`.asLongBuffer()
            recursiveRead(nextValues, offset + longs)
          }
        }
        recursiveRead(values, 0)
        LongBuffer.wrap(results)
      }
    } else {
      Index.emptyBuffer
    }
  }

}
