package io.georocket.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * A WriteStream that pipes everything to a ReadStream
 * @author Michel Kraemer
 * @param <S> the type of the elements that can be written to the WriteStream
 * @param <D> the type of the elements that can be read from the ReadStream
 */
public abstract class PipeStream<S, D> implements WriteStream<S>, ReadStream<D> {
  private static final Logger log = LoggerFactory.getLogger(PipeStream.class);
  
  protected boolean paused = false;
  protected Handler<Throwable> exceptionHandler;
  protected Handler<Void> drainHandler;
  protected Handler<D> dataHandler;
  protected Handler<Void> endHandler;
  
  /**
   * Will be called when a peer wants to read data
   */
  protected abstract void doRead();
  
  /**
   * Will be called when a peer writes data
   * @param data the data to write
   * @param handler will be called when the data has been written
   */
  protected abstract void doWrite(S data, Handler<AsyncResult<Void>> handler);
  
  /**
   * If a drain handler is configured, call it and reset it
   */
  protected void doDrain() {
    if (drainHandler != null) {
      Handler<Void> handler = drainHandler;
      drainHandler = null;
      handler.handle(null);
    }
  }
  
  /**
   * Call the exception handler or log the given exception
   * @param e the exception
   */
  protected void handleException(Throwable e) {
    if (exceptionHandler != null) {
      exceptionHandler.handle(e);
    } else {
      log.error("Unhandled exception", e);
    }
  }
  
  /**
   * Call the end handler
   */
  protected void handleEnd() {
    if (endHandler != null) {
      endHandler.handle(null);
    }
  }
  
  @Override
  public PipeStream<S, D> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public PipeStream<S, D> write(S data) {
    doWrite(data, ar -> {
      if (ar.failed()) {
        handleException(ar.cause());
      } else {
        doRead();
      }
    });
    return this;
  }

  @Override
  public PipeStream<S, D> setWriteQueueMaxSize(int maxSize) {
    throw new UnsupportedOperationException("PipeStream has no write queue");
  }
  
  @Override
  public boolean writeQueueFull() {
    return dataHandler == null;
  }

  @Override
  public PipeStream<S, D> drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    doDrain();
    return this;
  }

  @Override
  public PipeStream<S, D> handler(Handler<D> handler) {
    dataHandler = handler;
    if (dataHandler != null && !paused) {
      doRead();
    }
    return this;
  }

  @Override
  public PipeStream<S, D> pause() {
    paused = true;
    return this;
  }

  @Override
  public PipeStream<S, D> resume() {
    if (paused) {
      paused = false;
      if (dataHandler != null) {
        doRead();
      }
    }
    return this;
  }

  @Override
  public PipeStream<S, D> endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }
}
