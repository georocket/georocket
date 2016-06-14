package io.georocket.output;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

public abstract class AbstractMerger implements AsynchMerger {

  /* (non-Javadoc)
   * @see io.georocket.output.Merger#finishMerge(io.vertx.core.streams.WriteStream)
   */
  @Override
  public abstract void finishMerge(WriteStream<Buffer> out);

  /* (non-Javadoc)
   * @see io.georocket.output.Merger#merge(io.georocket.storage.ChunkReadStream, io.georocket.storage.ChunkMeta, io.vertx.core.streams.WriteStream, io.vertx.core.Handler)
   */
  @Override
  public abstract void merge(ChunkReadStream chunk, ChunkMeta meta, WriteStream<Buffer> out, Handler<AsyncResult<Void>> handler);

  /* (non-Javadoc)
   * @see io.georocket.output.Merger#init(io.georocket.storage.ChunkMeta, io.vertx.core.Handler)
   */
  @Override
  public abstract void init(ChunkMeta meta, Handler<AsyncResult<Void>> handler);

  /**
   * Initialize this merger and determine the merge strategy. This method
   * must be called for all chunks that should be merged. After
   * {@link #mergeObservable(ChunkReadStream, ChunkMeta, WriteStream)}
   * has been called this method must not be called any more.
   * @param meta the chunk metadata
   * @return an observable that completes once the merger has been
   * initialized with the given chunk
   */
  public Observable<Void> initObservable(ChunkMeta meta) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    init(meta, o.toHandler());
    return o;
  }
  
  /**
   * Merge a chunk using the current merge strategy. The given chunk should
   * have been passed to {@link #initObservable(ChunkMeta)} first. If it hasn't
   * the method may or may not accept it. If the chunk cannot be merged with
   * the current strategy, the returned observable will fail.
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @return an observable that completes once the chunk has been merged
   */
  public Observable<Void> mergeObservable(ChunkReadStream chunk, ChunkMeta meta, WriteStream<Buffer> out) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    merge(chunk, meta, out, o.toHandler());
    return o;
  }

}