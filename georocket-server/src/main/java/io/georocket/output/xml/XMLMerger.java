package io.georocket.output.xml;

import io.georocket.output.Merger;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.XMLChunkMeta;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import rx.Completable;

/**
 * Merges XML chunks using various strategies to create a valid XML document
 * @author Michel Kraemer
 */
public class XMLMerger implements Merger<XMLChunkMeta> {
  /**
   * The merger strategy determined by {@link #init(XMLChunkMeta)}
   */
  private MergeStrategy strategy;
  
  /**
   * {@code true} if {@link #merge(ChunkReadStream, XMLChunkMeta, WriteStream)}
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
  
  @Override
  public Completable init(XMLChunkMeta meta) {
    if (mergeStarted) {
      return Completable.error(new IllegalStateException("You cannot "
          + "initialize the merger anymore after merging has begun"));
    }
    
    if (strategy == null) {
      strategy = nextStrategy();
    }
    
    return strategy.canMerge(meta)
      .flatMapCompletable(canMerge -> {
        if (canMerge) {
          // current strategy is able to handle the chunk
          return strategy.init(meta);
        }
        
        // current strategy cannot merge the chunk. select next one and retry.
        MergeStrategy ns = nextStrategy();
        if (ns == null) {
          return Completable.error(new UnsupportedOperationException(
              "Cannot merge chunks. No valid strategy available."));
        }
        ns.setParents(strategy.getParents());
        strategy = ns;
        return init(meta);
      });
  }
  
  @Override
  public Completable merge(ChunkReadStream chunk, XMLChunkMeta meta,
      WriteStream<Buffer> out) {
    mergeStarted = true;
    if (strategy == null) {
      return Completable.error(new IllegalStateException(
          "You must call init() at least once"));
    }
    return strategy.merge(chunk, meta, out);
  }
  
  @Override
  public void finish(WriteStream<Buffer> out) {
    strategy.finish(out);
  }
}
