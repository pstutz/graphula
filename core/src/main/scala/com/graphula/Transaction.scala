package com.graphula

import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue

import scala.annotation.tailrec

import org.lmdbjava.Cursor
import org.lmdbjava.Dbi
import org.lmdbjava.Env
import org.lmdbjava.Txn

import com.graphula.util.HybridIterator

final class Transaction(env: Env[ByteBuffer], db: Dbi[ByteBuffer]) {

  private val readTxnPool = new LinkedBlockingQueue[Txn[ByteBuffer]]
  private val threadLocalCursor = new ThreadLocal[Cursor[ByteBuffer]] {
    override protected def initialValue(): Cursor[ByteBuffer] = {
      val txn = env.txnRead()
      val c = db.openCursor(txn)
      txn.close()
      c
    }
  }
  private val threadLocalRead = new ThreadLocal[Txn[ByteBuffer]] {
    override protected def initialValue(): Txn[ByteBuffer] = {
      val txn = env.txnRead()
      txn.reset()
      txn
    }
  }

  @inline def read(): Txn[ByteBuffer] = {
    Option(readTxnPool.poll())
      .map { txn =>
        txn.renew(); txn
      }
      .getOrElse(env.txnRead())
  }

  @inline def releaseRead(implicit txn: Txn[ByteBuffer]): Unit = {
    txn.reset()
    readTxnPool.put(txn)
  }

  @inline def withRead[G](block: Txn[ByteBuffer] => G): G = {
    implicit val txn = read()
    try {
      block(txn)
    } finally {
      releaseRead(txn)
    }
  }

  @inline def withThreadLocalRead[G](block: Txn[ByteBuffer] => G): G = {
    implicit val txn = threadLocalRead.get
    try {
      txn.renew()
      block(txn)
    } finally {
      txn.reset()
    }
  }

  @inline def write(): Txn[ByteBuffer] = {
    env.txnWrite()
  }

  @inline def releaseWrite(implicit txn: Txn[ByteBuffer]): Unit = {
    txn.close
  }

  @inline def withWrite[G](block: Txn[ByteBuffer] => G): G = {
    val txn = write()
    try {
      block(txn)
    } finally {
      txn.commit()
      releaseWrite(txn)
    }
  }

  @inline
  def withCursor[G](block: Cursor[ByteBuffer] => G)(
    implicit
    txn: Txn[ByteBuffer]
  ): G = {
    val c = threadLocalCursor.get
    c.renew(txn)
    block(c)
  }

  /**
   * Requires a write transaction.
   */
  @inline
  def withWriteCursor[G](block: Cursor[ByteBuffer] => G)(
    implicit
    txn: Txn[ByteBuffer]
  ): G = {
    val c = db.openCursor(txn)
    val r = block(c)
    c.close()
    r
  }

  @tailrec def close(): Unit = {
    Option(readTxnPool.poll()) match {
      case None => // We're done.
      case Some(txn) =>
        txn.close()
        close()
    }
  }

}
