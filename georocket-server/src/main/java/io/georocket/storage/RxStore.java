package io.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;
import rx.Single;

import java.util.List;
import java.util.Map;

/**
 * Wraps around {@link Store} and adds methods to be used with RxJava
 * @author Michel Kraemer
 */
public class RxStore implements Store {
  private final Store delegate;
  
  /**
   * Create a new rx-ified store
   * @param delegate the actual store to delegate to
   */
  public RxStore(Store delegate) {
    this.delegate = delegate;
  }
  
  /**
   * @return the actual non-rx-ified store
   */
  public Store getDelegate() {
    return delegate;
  }
  
  @Override
  public void add(String chunk, ChunkMeta chunkMeta, String path,
      IndexMeta indexMeta, Handler<AsyncResult<Void>> handler) {
    delegate.add(chunk, chunkMeta, path, indexMeta, handler);
  }
  
  /**
   * Observable version of {@link #add(String, ChunkMeta, String, IndexMeta, Handler)}
   * @param chunk the chunk to add
   * @param chunkMeta the chunk's metadata
   * @param path the path where the chunk should be stored (may be null)
   * @param indexMeta metadata affecting the way the chunk will be indexed
   * @return an observable that emits exactly one item when the operation has completed
   */
  public Observable<Void> addObservable(String chunk, ChunkMeta chunkMeta,
      String path, IndexMeta indexMeta) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    add(chunk, chunkMeta, path, indexMeta, o.toHandler());
    return o;
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    delegate.getOne(path, handler);
  }
  
  /**
   * Observable version of {@link #getOne(String, Handler)}
   * @param path the absolute path to the chunk
   * @return an observable that will emit a read stream that can be used to
   * get the chunk's contents
   */
  public Observable<ChunkReadStream> getOneObservable(String path) {
    ObservableFuture<ChunkReadStream> o = RxHelper.observableFuture();
    getOne(path, o.toHandler());
    return o;
  }

  @Override
  public void delete(String search, String path, Handler<AsyncResult<Void>> handler) {
    delegate.delete(search, path, handler);
  }
  
  /**
   * Observable version of {@link #delete(String, String, Handler)}
   * @param search the search query
   * @param path the path where to search for the chunks (may be null)
   * @return an observable that emits exactly one item when the operation has completed
   */
  public Observable<Void> deleteObservable(String search, String path) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    delete(search, path, o.toHandler());
    return o;
  }

  @Override
  public void get(String search, String path, Handler<AsyncResult<StoreCursor>> handler) {
    delegate.get(search, path, handler);
  }

  /**
   * Observable version of {@link #get(String, String, Handler)}
   * @param search the search query
   * @param path the path where to search for the chunks (may be null)
   * @return an observable that emits a cursor that can be used to iterate
   * over all matched chunks
   */
  public Observable<StoreCursor> getObservable(String search, String path) {
    ObservableFuture<StoreCursor> o = RxHelper.observableFuture();
    get(search, path, o.toHandler());
    return o;
  }

  @Override
  public void scroll(String search, String path, int size, Handler<AsyncResult<StoreCursor>> handler) {
    delegate.scroll(search, path, size, handler);
  }

  /**
   * Observable version of {@link #scroll(String, String, int, Handler)}
   * @param search the search query
   * @param path the path where to search for the chunks (may be null)
   * @param size the number of elements to load
   * @return on observable that emits a cursor that can be used to iterate
   * over all matched chunks
   */
  public Observable<StoreCursor> scrollObservable(String search, String path, int size) {
    ObservableFuture<StoreCursor> o = RxHelper.observableFuture();
    scroll(search, path, size, o.toHandler());
    return o;
  }

  @Override
  public void scroll(String scrollId, Handler<AsyncResult<StoreCursor>> handler) {
    delegate.scroll(scrollId, handler);
  }

  /**
   * Scrollable version of {@link #scroll(String, Handler)}
   * @param scrollId The scrollId to load the chunks
   * @return on observable that emits a cursor that can be used to iterate
   * over all matched chunks
   */
  public Observable<StoreCursor> scrollObservable(String scrollId) {
    ObservableFuture<StoreCursor> o = RxHelper.observableFuture();
    scroll(scrollId, o.toHandler());
    return o;
  }

  @Override
  public void getAttributeValues(String search, String path, String attribute,
      Handler<AsyncResult<AsyncCursor<String>>> handler) {
    delegate.getAttributeValues(search, path, attribute, handler);
  }

  public Single<AsyncCursor<String>> rxGetAttributeValues(String search,
      String path, String attribute) {
    ObservableFuture<AsyncCursor<String>> o = RxHelper.observableFuture();
    getAttributeValues(search, path, attribute, o.toHandler());
    return o.toSingle();
  }

  @Override
  public void getPropertyValues(String search, String path, String property,
      Handler<AsyncResult<AsyncCursor<String>>> handler) {
    delegate.getPropertyValues(search, path, property, handler);
  }

  public Single<AsyncCursor<String>> rxGetPropertyValues(String search,
      String path, String property) {
    ObservableFuture<AsyncCursor<String>> o = RxHelper.observableFuture();
    getPropertyValues(search, path, property, o.toHandler());
    return o.toSingle();
  }

  @Override
  public void setProperties(String search, String path,
      Map<String, String> properties, Handler<AsyncResult<Void>> handler) {
    delegate.setProperties(search, path, properties, handler);
  }

  public Single<Void> rxSetProperties(String search, String path,
      Map<String, String> properties) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    setProperties(search, path, properties, o.toHandler());
    return o.toSingle();
  }

  @Override
  public void removeProperties(String search, String path,
      List<String> properties, Handler<AsyncResult<Void>> handler) {
    delegate.removeProperties(search, path, properties, handler);
  }

  public Single<Void> rxRemoveProperties(String search, String path,
      List<String> properties) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    removeProperties(search, path, properties, o.toHandler());
    return o.toSingle();
  }

  @Override
  public void appendTags(String search, String path, List<String> tags,
      Handler<AsyncResult<Void>> handler) {
    delegate.appendTags(search, path, tags, handler);
  }

  public Single<Void> rxAppendTags(String search, String path, List<String> tags) {
      ObservableFuture<Void> o = RxHelper.observableFuture();
    appendTags(search, path, tags, o.toHandler());
    return o.toSingle();
  }

  @Override
  public void removeTags(String search, String path, List<String> tags,
      Handler<AsyncResult<Void>> handler) {
    delegate.removeTags(search, path, tags, handler);
  }

  public Single<Void> rxRemoveTags(String search, String path, List<String> tags) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    removeTags(search, path, tags, o.toHandler());
    return o.toSingle();
  }
}
