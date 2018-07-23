package io.georocket.util.io;

import io.vertx.core.Handler;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.streams.ReadStream;
import rx.Observable;

/**
 * <p>A {@link io.vertx.core.streams.ReadStream} that delegates to another one but decompresses all
 * data with GZIP.</p>
 * <p>The code is loosely based on {@link java.util.zip.GZIPInputStream}</p>
 * <p>This is the rx-ified version of {@link GzipReadStream}</p>
 * @author Michel Kraemer
 */
public class RxGzipReadStream implements ReadStream<Buffer> {
  private final GzipReadStream delegate;
  private Observable<Buffer> observable;

  /**
   * Creates new stream that wraps around another one
   * @param delegate the stream to wrap around
   */
  public RxGzipReadStream(GzipReadStream delegate) {
    this.delegate = delegate;
  }

  /**
   * Creates new stream that wraps around another one
   * @param delegate the stream to wrap around
   */
  @SuppressWarnings("unchecked")
  public RxGzipReadStream(ReadStream<Buffer> delegate) {
    this(new GzipReadStream(delegate.getDelegate()));
  }

  @Override
  public GzipReadStream getDelegate() {
    return delegate;
  }

  @Override
  public RxGzipReadStream exceptionHandler(Handler<Throwable> handler) {
    delegate.exceptionHandler(handler);
    return this;
  }

  @Override
  public RxGzipReadStream handler(Handler<Buffer> handler) {
    delegate.handler(buf -> {
      handler.handle(Buffer.newInstance(buf));
    });
    return this;
  }

  @Override
  public RxGzipReadStream pause() {
    delegate.pause();
    return this;
  }

  @Override
  public RxGzipReadStream resume() {
    delegate.resume();
    return this;
  }

  @Override
  public RxGzipReadStream endHandler(Handler<Void> endHandler) {
    delegate.endHandler(endHandler);
    return this;
  }

  @Override
  public Observable<Buffer> toObservable() {
    if (this.observable == null) {
      this.observable = RxHelper.toObservable(this.delegate, Buffer::newInstance);
    }
    return this.observable;
  }
}
