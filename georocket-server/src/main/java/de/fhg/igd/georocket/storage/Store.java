package de.fhg.igd.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * A store for chunks
 * @author Michel Kraemer
 */
public interface Store {
  /**
   * Add a chunk to the store
   * @param chunk the chunk to add
   * @param meta the chunk's metadata
   * @param handler will be called when the chunk has been added to the store
   */
  void add(String chunk, ChunkMeta meta, Handler<AsyncResult<Void>> handler);
  
  /**
   * Get a chunk from the store. The returned {@link ChunkReadStream} must
   * be closed after use to release all resources.
   * @param name the chunk's name
   * @param handler will be called when the chunk has been retrieved from the store
   */
  void getOne(String name, Handler<AsyncResult<ChunkReadStream>> handler);
  
  /**
   * Get a number of chunks from the store using quick-search
   * @param search the quick-search query
   * @param handler will be called when the chunks have been retrieved from the store
   */
  void get(String search, Handler<AsyncResult<StoreCursor>> handler);
}
