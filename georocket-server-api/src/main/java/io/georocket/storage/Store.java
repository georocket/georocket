package io.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * A store for chunks
 * @since 1.0.0
 * @author Michel Kraemer
 */
public interface Store {
  /**
   * Add a chunk to the store
   * @param chunk the chunk to add
   * @param chunkMeta the chunk's metadata
   * @param path the path where the chunk should be stored (may be null)
   * @param indexMeta metadata affecting the way the chunk will be indexed
   * @param handler will be called when the chunk has been added to the store
   */
  void add(String chunk, ChunkMeta chunkMeta, String path, IndexMeta indexMeta,
      Handler<AsyncResult<Void>> handler);

  /**
   * Get a chunk from the store. The returned {@link ChunkReadStream} must
   * be closed after use to release all resources.
   * @param path the absolute path to the chunk
   * @param handler will be called when the chunk has been retrieved from the store
   */
  void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler);

  /**
   * Delete all chunks from the store that match a given query
   * @param search the search query
   * @param path the path where to search for the chunks (may be null)
   * @param handler will be called when the chunk has been deleted
   */
  void delete(String search, String path, Handler<AsyncResult<Void>> handler);

  /**
   * Get a number of chunks from the store using quick-search
   * @param search the search query
   * @param path the path where to search for the chunks (may be null)
   * @param handler will be called when the chunks have been retrieved from the store
   */
  void get(String search, String path, Handler<AsyncResult<StoreCursor>> handler);
  
  void scroll(String search, String path, Handler<AsyncResult<StoreCursor>> handler);
  
  void scroll(String scrollId, Handler<AsyncResult<StoreCursor>> handler);
}
