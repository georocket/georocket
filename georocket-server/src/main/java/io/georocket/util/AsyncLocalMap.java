package io.georocket.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.LocalMap;

/**
 * An implementation of {@link AsyncMap} that delegates to a {@link LocalMap}
 * @author Michel Kraemer
 * @param <K> the type of the keys in the map
 * @param <V> the type of the values in the map
 */
public class AsyncLocalMap<K, V> implements AsyncMap<K, V> {
  private final LocalMap<K, V> delegate;
  
  /**
   * Construct a new map
   * @param delegate the local map to delegate to
   */
  public AsyncLocalMap(LocalMap<K, V> delegate) {
    this.delegate = delegate;
  }
  
  @Override
  public void get(K k, Handler<AsyncResult<V>> handler) {
    handler.handle(Future.succeededFuture(delegate.get(k)));
  }

  @Override
  public void put(K k, V v, Handler<AsyncResult<Void>> handler) {
    delegate.put(k, v);
    handler.handle(Future.succeededFuture());
  }

  @Override
  public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> handler) {
    handler.handle(Future.failedFuture(new UnsupportedOperationException()));
  }

  @Override
  public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> handler) {
    delegate.putIfAbsent(k, v);
    handler.handle(Future.succeededFuture());
  }

  @Override
  public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> handler) {
    handler.handle(Future.failedFuture(new UnsupportedOperationException()));
  }

  @Override
  public void remove(K k, Handler<AsyncResult<V>> handler) {
    handler.handle(Future.succeededFuture(delegate.remove(k)));
  }

  @Override
  public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> handler) {
    handler.handle(Future.succeededFuture(delegate.removeIfPresent(k, v)));
  }

  @Override
  public void replace(K k, V v, Handler<AsyncResult<V>> handler) {
    handler.handle(Future.succeededFuture(delegate.replace(k, v)));
  }

  @Override
  public void replaceIfPresent(K k, V oldValue, V newValue,
      Handler<AsyncResult<Boolean>> handler) {
    handler.handle(Future.succeededFuture(delegate.replaceIfPresent(
      k, oldValue, newValue)));
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> handler) {
    delegate.clear();
    handler.handle(Future.succeededFuture());
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> handler) {
    handler.handle(Future.succeededFuture(delegate.size()));
  }
}
