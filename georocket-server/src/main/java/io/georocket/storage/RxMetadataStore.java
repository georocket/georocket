package io.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

/**
 * Wraps around {@link MetadataStore} and adds methods to be used with RxJava
 * @author Tim Hellhake
 */
public class RxMetadataStore implements MetadataStore {
  private final MetadataStore delegate;

  /**
   * Create a new rx-ified store
   * @param delegate the actual store to delegate to
   */
  public RxMetadataStore(MetadataStore delegate) {
    this.delegate = delegate;
  }

  @Override
  public void getPropertyValues(String search, String path, String property,
    Handler<AsyncResult<AsyncCursor<String>>> handler) {
    delegate.getPropertyValues(search, path, property, handler);
  }

  /**
   * Observable version of {@link #getPropertyValues(String, String, String, Handler)}
   * @param search the search query
   * @param path the path where to search for the values (may be null)
   * @return an observable that will emit a read stream that can be used to
   * get the found property values
   */
  public Observable<AsyncCursor<String>> rxGetPropertyValues(String search, String path,
    String property) {
    ObservableFuture<AsyncCursor<String>> o = RxHelper.observableFuture();
    getPropertyValues(search, path, property, o.toHandler());
    return o;
  }
}
