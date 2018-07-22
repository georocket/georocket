package io.georocket.util.io;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;

/**
 * A ReadStream that delegates to another one
 * @author Michel Kraemer
 * @param <T> the type of the objects that can be read from the stream
 */
public class DelegateReadStream<T> implements ReadStream<T> {
  protected final ReadStream<T> delegate;

  /**
   * Constructs a new read stream
   * @param delegate the stream to delegate to
   */
  public DelegateReadStream(ReadStream<T> delegate) {
    this.delegate = delegate;
  }
  
  @Override
  public ReadStream<T> exceptionHandler(Handler<Throwable> handler) {
    delegate.exceptionHandler(handler);
    return this;
  }

  @Override
  public ReadStream<T> handler(Handler<T> handler) {
    delegate.handler(handler);
    return this;
  }

  @Override
  public ReadStream<T> pause() {
    delegate.pause();
    return this;
  }

  @Override
  public ReadStream<T> resume() {
    delegate.resume();
    return this;
  }

  @Override
  public ReadStream<T> endHandler(Handler<Void> endHandler) {
    delegate.endHandler(endHandler);
    return this;
  }
}
