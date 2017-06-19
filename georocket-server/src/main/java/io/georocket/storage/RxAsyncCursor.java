package io.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import rx.Observable;
import rx.Producer;
import rx.internal.operators.BackpressureUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Wraps around {@link AsyncCursor} so it can be used with RxJava
 * @author Tim Hellhake
 * @param <T> type of the cursor item
 */
public class RxAsyncCursor<T> implements AsyncCursor<T> {
  private final AsyncCursor<T> delegate;

  /**
   * Create a new rx-ified cursor
   * @param delegate the actual cursor to delegate to
   */
  public RxAsyncCursor(AsyncCursor<T> delegate) {
    this.delegate = delegate;
  }

  /**
   * @return the actual non-rx-ified cursor
   */
  public AsyncCursor<T> getDelegate() {
    return delegate;
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public void next(Handler<AsyncResult<T>> handler) {
    delegate.next(handler);
  }

  /**
   * Convert this cursor to an observable
   * @return an observable emitting the items from the cursor
   */
  public Observable<T> toObservable() {
    return Observable.unsafeCreate(s -> {
      s.setProducer(new Producer() {
        private AtomicLong requested = new AtomicLong();

        @Override
        public void request(long n) {
          if (n > 0 && !s.isUnsubscribed() &&
            BackpressureUtils.getAndAddRequest(requested, n) == 0) {
            drain();
          }
        }

        private void drain() {
          if (requested.get() > 0) {
            if (!hasNext()) {
              if (!s.isUnsubscribed()) {
                s.onCompleted();
              }
              return;
            }

            next(ar -> {
              if (s.isUnsubscribed()) {
                return;
              }
              if (ar.failed()) {
                s.onError(ar.cause());
              } else {
                s.onNext(ar.result());
                requested.decrementAndGet();
                drain();
              }
            });
          }
        }
      });
    });
  }
}