package io.georocket.util.io;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * A {@link WriteStream} that collects all written data in a {@link Buffer}
 * @author Michel Kraemer
 */
public class BufferWriteStream implements WriteStream<Buffer> {
  private final Buffer buf = Buffer.buffer();
  
  @Override
  public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
    // exceptions cannot happen
    return this;
  }

  @Override
  public WriteStream<Buffer> write(Buffer data) {
    return write(data, null);
  }

  @Override
  public WriteStream<Buffer> write(Buffer data, Handler<AsyncResult<Void>> handler) {
    buf.appendBuffer(data);
    if (handler != null) {
      handler.handle(Future.succeededFuture());
    }
    return this;
  }

  @Override
  public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
    return this; // ignore
  }

  @Override
  public boolean writeQueueFull() {
    return false; // never full
  }

  @Override
  public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
    // we don't need a drain handler because we're never full
    return this;
  }
  
  @Override
  public void end() {
    end((Handler<AsyncResult<Void>>)null);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    if (handler != null) {
      handler.handle(Future.succeededFuture());
    }
  }
  
  /**
   * @return the buffer
   */
  public Buffer getBuffer() {
    return buf;
  }
}
