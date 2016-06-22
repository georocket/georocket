package io.georocket.output;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Merges XML chunks using various strategies to create a valid XML document
 * @author Michel Kraemer
 */
public class XMLMerger extends AbstractMerger{
  /**
   * The merger strategy determined by {@link #init(ChunkMeta, Handler)}
   */
  private XMLMergeStrategy strategy;
  
  /**
   * {@code true} if {@link #merge(ChunkReadStream, ChunkMeta, WriteStream, Handler)}
   * has been called at least once
   */
  private boolean mergeStarted = false;
  
  /**
   * @return the next merge strategy (depending on the current one) or
   * {@code null} if there is no other strategy available.
   */
  private XMLMergeStrategy nextStrategy() {
    if (strategy == null) {
      return new AllSameStrategy();
    } else if (strategy instanceof AllSameStrategy) {
      return new MergeNamespacesStrategy();
    }
    return null;
  }
  
  /* (non-Javadoc)
   * @see io.georocket.output.Merger#init(io.georocket.storage.ChunkMeta, io.vertx.core.Handler)
   */
  @Override
  public void init(ChunkMeta meta, Handler<AsyncResult<Void>> handler) {
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
      XMLMergeStrategy ns = nextStrategy();
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
  
  /* (non-Javadoc)
   * @see io.georocket.output.Merger#merge(io.georocket.storage.ChunkReadStream, io.georocket.storage.ChunkMeta, io.vertx.core.streams.WriteStream, io.vertx.core.Handler)
   */
  @Override
  public void merge(ChunkReadStream chunk, ChunkMeta meta, WriteStream<Buffer> out,
      Handler<AsyncResult<Void>> handler) {
    mergeStarted = true;
    if (strategy == null) {
      handler.handle(Future.failedFuture(new IllegalStateException(
          "You must call init() at least once")));
    } else {
      strategy.merge(chunk, meta, out, handler);
    }
  }
  
  /* (non-Javadoc)
   * @see io.georocket.output.Merger#finishMerge(io.vertx.core.streams.WriteStream)
   */
  @Override
  public void finishMerge(WriteStream<Buffer> out) {
    strategy.finishMerge(out);
  }
}
