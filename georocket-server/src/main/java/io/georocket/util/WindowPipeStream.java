package io.georocket.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

/**
 * A {@link PipeStream} that records transferred data in a {@link Window}
 * @author Michel Kraemer
 */
public class WindowPipeStream extends PipeStream<Buffer, Buffer> {
  private Window window = new Window();
  private Buffer data;
  
  @Override
  protected void doRead() {
    while (!paused && dataHandler != null) {
      if (data == null) {
        // wait for more input
        doDrain();
        break;
      }
      
      Buffer d = data;
      data = null;
      dataHandler.handle(d);
    }
  }

  @Override
  protected void doWrite(Buffer data, Handler<AsyncResult<Void>> handler) {
    if (this.data != null) {
      handler.handle(Future.failedFuture(new IllegalStateException("Still some data to write")));
    } else {
      this.data = data;
      window.append(data);
      handler.handle(Future.succeededFuture());
    }
  }
  
  @Override
  protected void doDrain() {
    if (data == null) {
      super.doDrain();
    }
  }
  
  @Override
  public boolean writeQueueFull() {
    return super.writeQueueFull() || data != null;
  }
  
  /**
   * @return the window that records all transferred data
   */
  public Window getWindow() {
    return window;
  }
}
