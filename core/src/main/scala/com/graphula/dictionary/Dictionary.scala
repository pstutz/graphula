package com.graphula.dictionary

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec

import org.lmdbjava.DbiFlags.MDB_CREATE
import org.lmdbjava.Env
import org.lmdbjava.GetOp.MDB_SET
import org.lmdbjava.GetOp.MDB_SET_KEY
import org.lmdbjava.PutFlags.MDB_NOOVERWRITE
import org.lmdbjava.Txn

import com.graphula.Transaction
import com.graphula.util.ThreadLocalSizedBuffer

import net.jpountz.xxhash.XXHash64
import net.jpountz.xxhash.XXHashFactory

final object Dictionary {

  private val initialStringBufferSize = 32

}

// TODO: Ensure that no string can have id 0 assigned by the dictionary
final class Dictionary(
    env: Env[ByteBuffer],
    hasher: XXHash64 = XXHashFactory.fastestJavaInstance().hash64()
) {

  private def hash(bytes: Array[Byte]): Long = {
    val l = bytes.length
    hasher.hash(bytes, 0, l, l) & Long.MaxValue
  }

  private val id2String = env.openDbi(
    "Id2String",
    MDB_CREATE
  )

  private val string2Id = env.openDbi(
    "String2Id",
    MDB_CREATE
  )

  val id2StringTransaction = new Transaction(env, id2String)

  def close(): Unit = {
    id2StringTransaction.close()
  }

  private val allIdsTakenUpTo = new AtomicLong(0)

  private val charset = Charset.defaultCharset

  private val longSize = 8
  private val longBuffer = new ThreadLocalSizedBuffer(longSize)

  private val threadLocalStringBuffer = new ThreadLocalSizedBuffer(
    Dictionary.initialStringBufferSize
  )

  init()

  /**
   * The empty string needs to own its hash code in the Long -> String map,
   * because it is not a legal key for the String -> Long exception map.
   */
  def init(): Unit = {
    id2StringTransaction.withWrite { implicit txn =>
      add("")
    }
  }

  private def toStringBuffer(b: Array[Byte]): ByteBuffer = {
    val buffer = threadLocalStringBuffer.getSize(b.length)
    buffer.put(b)
    buffer.flip()
    buffer
  }

  /**
   * Returns the hash of `s`.
   */
  def hash(s: String): Long = {
    val stringBytes = s.getBytes(charset)
    hash(stringBytes)
  }

  private def longKey(id: Long): ByteBuffer = {
    val idBuffer = longBuffer.get
    idBuffer.putLong(0, id)
    idBuffer
  }

  /**
   * Decodes an encoded long, returns the decoded String or null if
   * it did not exist.
   */
  def apply(id: Long)(implicit txn: Txn[ByteBuffer]): String = {
    id2StringTransaction.withCursor { c =>
      val found = c.get(longKey(id), MDB_SET_KEY)
      if (!found) {
        if (id > 0 && id <= allIdsTakenUpTo.get) {
          s"_:$id" // Blank node id.
        } else {
          null
        }
      } else {
        val stringBuffer = c.`val`
        val stringBytes = new Array[Byte](stringBuffer.remaining())
        stringBuffer.get(stringBytes)
        new String(stringBytes, charset)
      }
    }
  }

  /**
   * Returns the id for string `s` or 0 if it is not contained.
   */
  // TODO: What if hash is 0?
  def apply(s: String)(implicit txn: Txn[ByteBuffer]): Long = {
    val stringBytes = s.getBytes(charset)
    val idCandidate = hash(stringBytes)
    val existing = id2String.get(txn, longKey(idCandidate))
    if (existing == null) {
      0
    } else {
      val stringBuffer = toStringBuffer(stringBytes)
      if (stringBuffer.equals(existing)) {
        idCandidate
      } else {
        idFromExceptions(stringBuffer)
      }
    }
  }

  /**
   * Requires a write transaction.
   */
  def add(s: String)(implicit txn: Txn[ByteBuffer]): Long = {
    assert(s != null)
    val stringBytes = s.getBytes(charset)
    val idCandidate = hash(stringBytes)
    val idBuffer = longKey(idCandidate)
    val existing = id2String.get(txn, idBuffer)
    val stringBuffer = toStringBuffer(stringBytes)
    if (existing == null && !isBlankNode(idCandidate)) {
      // New entry.
      addNewEntryThatOwnsHash(stringBuffer, idCandidate, idBuffer)
    } else if (stringBuffer.equals(existing)) {
      // Same string was added before.
      idCandidate
    } else {
      // New entry, but another previously added string has the same hash.
      addEntryToExceptions(stringBuffer)
    }
  }

  /**
   * Requires a write transaction.
   */
  private def addNewEntryThatOwnsHash(
    stringBuffer: ByteBuffer,
    idCandidate: Long,
    idBuffer: ByteBuffer
  )(implicit txn: Txn[ByteBuffer]): Long = {
    val wasPut = id2String.put(txn, idBuffer, stringBuffer, MDB_NOOVERWRITE)
    if (wasPut) {
      idCandidate
    } else {
      val value = txn.`val`
      if (!stringBuffer.equals(value)) {
        addEntryToExceptions(stringBuffer)
      } else {
        idCandidate
      }
    }
  }

  /**
   * Requires a write transaction.
   */
  private def addEntryToExceptions(stringBuffer: ByteBuffer)(
    implicit
    txn: Txn[ByteBuffer]
  ): Long = {
    val exceptionIdBuffer = string2Id.get(txn, stringBuffer)
    if (exceptionIdBuffer != null) {
      exceptionIdBuffer.getLong()
    } else {
      val attemptedIdBuffer = longBuffer.get
      @tailrec def recursiveAddEntryToExceptions(): Long = {
        val attemptedId = allIdsTakenUpTo.incrementAndGet
        attemptedIdBuffer.putLong(0, attemptedId)
        val wasPut =
          id2String.put(txn, attemptedIdBuffer, stringBuffer, MDB_NOOVERWRITE)
        if (wasPut) {
          attemptedId
        } else {
          recursiveAddEntryToExceptions()
        }
      }
      val id = recursiveAddEntryToExceptions()
      string2Id.put(txn, stringBuffer, attemptedIdBuffer)
      id
    }
  }

  /**
   * Requires a read transaction.
   * Returns the assigned id if there is an entry and 0 otherwise.
   */
  private def idFromExceptions(stringBuffer: ByteBuffer)(
    implicit
    txn: Txn[ByteBuffer]
  ): Long = {
    val idBuffer = string2Id.get(txn, stringBuffer)
    if (!idBuffer.hasRemaining()) {
      0
    } else {
      idBuffer.getLong()
    }
  }

  /**
   * Get an unused id that is not associated with a string.
   */
  def getBlankNode(): Long = {
    id2StringTransaction.withThreadLocalRead { implicit txn =>
      id2StringTransaction.withCursor { c =>
        val idBuffer = longBuffer.get
        @tailrec def getBlankNodeIdRec(): Long = {
          val attemptedId = allIdsTakenUpTo.incrementAndGet
          idBuffer.putLong(0, attemptedId)
          val idInUse = c.get(idBuffer, MDB_SET)
          if (!idInUse) {
            attemptedId
          } else {
            getBlankNodeIdRec()
          }
        }
        getBlankNodeIdRec()
      }
    }
  }

  /**
   * Returns true if `id` is a blank node.
   */
  def isBlankNode(id: Long)(implicit txn: Txn[ByteBuffer]): Boolean = {
    id > 0 && id <= allIdsTakenUpTo.get && {
      id2StringTransaction.withCursor { c =>
        val existsString = c.get(longKey(id), MDB_SET)
        !existsString
      }
    }
  }

}
