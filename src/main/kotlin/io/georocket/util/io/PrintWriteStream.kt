package io.georocket.util.io

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import java.io.PrintWriter

/**
 * A [WriteStream] that forwards all [Buffer]s to the given [writer]
 * @author Michel Kraemer
 */
class PrintWriteStream(private val writer: PrintWriter) : WriteStream<Buffer> {
  override fun exceptionHandler(handler: Handler<Throwable>?): WriteStream<Buffer> {
    // exceptions cannot happen
    return this
  }

  override fun write(data: Buffer): WriteStream<Buffer> {
    return write(data, null)
  }

  override fun write(data: Buffer, handler: Handler<AsyncResult<Void>>?): WriteStream<Buffer> {
    writer.write(data.toString())
    handler?.handle(Future.succeededFuture())
    return this
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

  override fun end() {
    end(null as Handler<AsyncResult<Void>>?)
  }

  override fun end(handler: Handler<AsyncResult<Void>>?) {
    handler?.handle(Future.succeededFuture())
  }
}
