package io.georocket.output.xml;

import io.georocket.storage.XMLChunkMeta;
import io.georocket.output.Merger;
import io.georocket.storage.ChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

/**
 * Merges XML chunks using various strategies to create a valid XML document
 * @author Michel Kraemer
 */
public class XMLMerger implements Merger<XMLChunkMeta> {
  /**
   * The merger strategy determined by {@link #init(XMLChunkMeta, Handler)}
   */
  private MergeStrategy strategy;
  
  /**
   * {@code true} if {@link #merge(ChunkReadStream, XMLChunkMeta, WriteStream, Handler)}
   * has been called at least once
   */
  private boolean mergeStarted = false;
  
  /**
   * @return the next merge strategy (depending on the current one) or
   * {@code null} if there is no other strategy available.
   */
  private MergeStrategy nextStrategy() {
    if (strategy == null) {
      return new AllSameStrategy();
    } else if (strategy instanceof AllSameStrategy) {
      return new MergeNamespacesStrategy();
    }
    return null;
  }
  
  /**
   * Initialize this merger and determine the merge strategy. This method
   * must be called for all chunks that should be merged. After
   * {@link #merge(ChunkReadStream, XMLChunkMeta, WriteStream, Handler)}
   * has been called this method must not be called any more.
   * @param meta the chunk metadata
   * @param handler will be called when the merger has been initialized with
   * the given chunk
   */
  public void init(XMLChunkMeta meta, Handler<AsyncResult<Void>> handler) {
    if (mergeStarted) {
      handler.handle(Future.failedFuture(new IllegalStateException("You cannot "
          + "initialize the merger anymore after merging has begun")));
      return;
    }
    
    if (strategy == null) {
      strategy = nextStrategy();
    }
    
    strategy.canMerge(meta, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      
      if (ar.result()) {
        // current strategy is able to handle the chunk
        strategy.init(meta, handler);
        return;
      }
      
      // current strategy cannot merge the chunk. select next one and retry.
      MergeStrategy ns = nextStrategy();
      if (ns == null) {
        handler.handle(Future.failedFuture(new UnsupportedOperationException(
            "Cannot merge chunks. No valid strategy available.")));
      } else {
        ns.setParents(strategy.getParents());
        strategy = ns;
        init(meta, handler);
      }
    });
  }
  
  @Override
  public Observable<Void> init(XMLChunkMeta meta) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    init(meta, o.toHandler());
    return o;
  }
  
  /**
   * Merge a chunk using the current merge strategy. The given chunk should
   * have been passed to {@link #init(XMLChunkMeta, Handler)} first. If it hasn't
   * the method may or may not accept it. If the chunk cannot be merged with
   * the current strategy, the method will call the given handler with a
   * failed result.
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @param handler will be called when the chunk has been merged
   */
  public void merge(ChunkReadStream chunk, XMLChunkMeta meta, WriteStream<Buffer> out,
      Handler<AsyncResult<Void>> handler) {
    mergeStarted = true;
    if (strategy == null) {
      handler.handle(Future.failedFuture(new IllegalStateException(
          "You must call init() at least once")));
    } else {
      strategy.merge(chunk, meta, out, handler);
    }
  }
  
  @Override
  public Observable<Void> merge(ChunkReadStream chunk, XMLChunkMeta meta,
      WriteStream<Buffer> out) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    merge(chunk, meta, out, o.toHandler());
    return o;
  }
  
  @Override
  public void finish(WriteStream<Buffer> out) {
    strategy.finishMerge(out);
  }
}
