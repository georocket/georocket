package io.georocket.util.io

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream

/**
 * A [WriteStream] that collects all written data in a [Buffer]
 * @author Michel Kraemer
 */
class BufferWriteStream : WriteStream<Buffer> {
  /**
   * @return the buffer
   */
  val buffer = Buffer.buffer()

  override fun exceptionHandler(handler: Handler<Throwable>): WriteStream<Buffer> {
    // exceptions cannot happen
    return this
  }

  override fun write(data: Buffer): Future<Void> {
    val promise = Promise.promise<Void>()
    write(data, promise)
    return promise.future()
  }

  override fun write(data: Buffer, handler: Handler<AsyncResult<Void>>) {
    buffer.appendBuffer(data)
    handler.handle(Future.succeededFuture())
  }

  override fun setWriteQueueMaxSize(maxSize: Int): WriteStream<Buffer> {
    return this // ignore
  }

  override fun writeQueueFull(): Boolean {
    return false // never full
  }

  override fun drainHandler(handler: Handler<Void>): WriteStream<Buffer> {
    // we don't need a drain handler because we're never full
    return this
  }

  override fun end(): Future<Void> {
    val promise = Promise.promise<Void>()
    end(promise)
    return promise.future()
  }

  override fun end(handler: Handler<AsyncResult<Void>>) {
    handler.handle(Future.succeededFuture())
  }
}
