package io.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * A cursor that can be used to iterate over chunks in a {@link LegacyStore}
 * @since 1.0.0
 * @author Michel Kraemer
 */
public interface LegacyStoreCursor extends LegacyAsyncCursor<ChunkMeta> {
  /**
   * @return true if there are more items to iterate over
   */
  boolean hasNext();
  
  /**
   * Get the next item from the store
   * @param handler will be called when the item has been retrieved
   */
  void next(Handler<AsyncResult<ChunkMeta>> handler);
  
  /**
   * @return the absolute path to the chunk that has been produced by the last
   * call to {@link #next(Handler)}
   */
  String getChunkPath();

  /**
   * @return info about the set of items
   */
  CursorInfo getInfo();
}
