package io.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;
import java.util.Map;

/**
 * A store for chunks
 * @since 1.0.0
 * @author Michel Kraemer
 */
@Deprecated
public interface LegacyStore {
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
   * @deprecated Call {@link #delete(String, String, DeleteMeta, Handler)}
   * instead with a unique {@code correlationId} in the {@link DeleteMeta}
   * object so the deletion process can be tracked correctly. This method
   * will be removed in GeoRocket 2.0.0.
   */
  @Deprecated
  void delete(String search, String path, Handler<AsyncResult<Void>> handler);

  /**
   * Delete all chunks from the store that match a given query. Callers should
   * pass a {@link DeleteMeta} object with a unique {@code correlationId} so the
   * deletion process can be tracked correctly.
   * @param search the search query
   * @param path the path where to search for the chunks (may be null)
   * @param deleteMeta a metadata object containing additional information
   * about the deletion process
   * @param handler will be called when the chunk has been deleted
   * @since 1.4.0
   */
  void delete(String search, String path, DeleteMeta deleteMeta,
      Handler<AsyncResult<Void>> handler);

  /**
   * Get a number of chunks from the store using quick-search
   * @param search the search query
   * @param path the path where to search for the chunks (may be null)
   * @param handler will be called when the chunks have been retrieved from the store
   */
  void get(String search, String path, Handler<AsyncResult<LegacyStoreCursor>> handler);

  /**
   * Start scrolling but load only one frame.
   * @param search the search query
   * @param path the path where to search for the chunks (may be null)
   * @param handler will be called when the chunks have been retrieved from the store
   * @param size the number of elements to load.
   */
  void scroll(String search, String path, int size, Handler<AsyncResult<LegacyStoreCursor>> handler);

  /**
   * Continue scrolling with a given scrollId
   * @param scrollId The scrollId to load the chunks
   * @param handler will be called when the chunks have been retrieved from the store
   */
  void scroll(String scrollId, Handler<AsyncResult<LegacyStoreCursor>> handler);

  /**
   * Get all values for the specified attribute
   * @param search the search query
   * @param path the path where to search for the values (may be null)
   * @param attribute the name of the attribute
   * @param handler will be called when the values have been retrieved from the store
   */
  void getAttributeValues(String search, String path, String attribute,
    Handler<AsyncResult<LegacyAsyncCursor<Object>>> handler);

  /**
   * Get all values for the specified property
   * @param search the search query
   * @param path the path where to search for the values (may be null)
   * @param property the name of the property
   * @param handler will be called when the values have been retrieved from the store
   */
  void getPropertyValues(String search, String path, String property,
    Handler<AsyncResult<LegacyAsyncCursor<String>>> handler);

  /**
   * Set the properties of a list of chunks selected by search and path
   * @param search the search query
   * @param path the path where to search for the values (may be null)
   * @param properties the list of properties to set
   * @param handler will be called when the properties are set
   */
  void setProperties(String search, String path, Map<String, String> properties,
    Handler<AsyncResult<Void>> handler);

  /**
   * Remove the properties of a list of chunks selected by search and path
   * @param search the search query
   * @param path the path where to search for the values (may be null)
   * @param properties the list of properties to remove
   * @param handler will be called when the properties are removed
   */
  void removeProperties(String search, String path, List<String> properties,
    Handler<AsyncResult<Void>> handler);

  /**
   * Append tags to of a list of chunks selected by search and path
   * @param search the search query
   * @param path the path where to search for the values (may be null)
   * @param tags the list of tags to append
   * @param handler will be called when the tags are appended
   */
  void appendTags(String search, String path, List<String> tags,
    Handler<AsyncResult<Void>> handler);

  /**
   * Remove the tags of a list of chunks selected by search and path
   * @param search the search query
   * @param path the path where to search for the values (may be null)
   * @param tags the list of tags to remove
   * @param handler will be called when the tags are removed
   */
  void removeTags(String search, String path, List<String> tags,
    Handler<AsyncResult<Void>> handler);
}
