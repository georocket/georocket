package io.georocket.util.io;

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
    buf.appendBuffer(data);
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
    // nothing to do here
  }
  
  /**
   * @return the buffer
   */
  public Buffer getBuffer() {
    return buf;
  }
}
