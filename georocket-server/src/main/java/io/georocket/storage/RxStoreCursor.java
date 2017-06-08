package io.georocket.storage;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import rx.Observable;
import rx.Producer;
import rx.internal.operators.BackpressureUtils;

/**
 * Wraps around {@link StoreCursor} so it can be used with RxJava
 * @author Michel Kraemer
 */
public class RxStoreCursor implements StoreCursor {
  private final StoreCursor delegate;

  /**
   * Create a new rx-ified cursor
   * @param delegate the actual cursor to delegate to
   */
  public RxStoreCursor(StoreCursor delegate) {
    this.delegate = delegate;
  }
  
  /**
   * @return the actual non-rx-ified cursor
   */
  public StoreCursor getDelegate() {
    return delegate;
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public void next(Handler<AsyncResult<ChunkMeta>> handler) {
    delegate.next(handler);
  }

  @Override
  public String getChunkPath() {
    return delegate.getChunkPath();
  }

  @Override
  public CursorInfo getInfo() {
    return delegate.getInfo();
  }

  /**
   * Convert this cursor to an observable
   * @return an observable emitting the chunks from the cursor and their
   * respective path in the store (retrieved via {@link #getChunkPath()})
   */
  public Observable<Pair<ChunkMeta, String>> toObservable() {
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
                s.onNext(Pair.of(ar.result(), getChunkPath()));
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
