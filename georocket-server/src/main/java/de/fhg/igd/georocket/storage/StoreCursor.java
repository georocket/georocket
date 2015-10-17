package de.fhg.igd.georocket.storage;

import de.fhg.igd.georocket.util.ChunkMeta;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * A cursor that can be used to iterate over chunks in a {@link Store}
 * @author Michel Kraemer
 */
public interface StoreCursor {
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
   * @return the ID of the chunk that has been produced by the last
   * call to {@link #next(Handler)}
   */
  String getChunkId();
  
  /**
   * Open the chunk that has been produced by the last call to {@link #next(Handler)}
   * @param handler will be called when the chunk has been opened
   */
  void openChunk(Handler<AsyncResult<ChunkReadStream>> handler);
}
