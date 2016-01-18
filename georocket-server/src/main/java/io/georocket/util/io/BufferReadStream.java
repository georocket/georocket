package io.georocket.util.io;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * A {@link ReadStream} that reads from a buffer
 * @author Michel Kraemer
 */
public class BufferReadStream implements ReadStream<Buffer> {
  private Buffer buf;
  private boolean paused;
  private Handler<Buffer> handler;
  private Handler<Void> endHandler;
  
  /**
   * Constructs a stream
   * @param buf the buffer to read from
   */
  public BufferReadStream(Buffer buf) {
    this.buf = buf;
  }
  
  @Override
  public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
    // exceptions can never happen
    return this;
  }
  
  private void doWrite(Handler<Buffer> handler) {
    handler.handle(buf);
    buf = null;
    if (endHandler != null) {
      endHandler.handle(null);
    }
  }

  @Override
  public ReadStream<Buffer> handler(Handler<Buffer> handler) {
    if (paused) {
      this.handler = handler;
    } else {
      doWrite(handler);
    }
    return this;
  }

  @Override
  public ReadStream<Buffer> pause() {
    this.paused = true;
    return this;
  }

  @Override
  public ReadStream<Buffer> resume() {
    if (paused) {
      paused = false;
      if (handler != null) {
        Handler<Buffer> h = handler;
        handler = null;
        doWrite(h);
      }
    }
    return this;
  }

  @Override
  public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
    if (buf == null) {
      endHandler.handle(null);
    } else {
      this.endHandler = endHandler;
    }
    return this;
  }
}
