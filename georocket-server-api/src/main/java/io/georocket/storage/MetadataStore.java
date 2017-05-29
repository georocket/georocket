package io.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * A store for metadata
 * @since 1.1.0
 * @author Tim Hellhake
 */
public interface MetadataStore {
  /**
   * Get all values for the specified property
   * @param search the search query
   * @param path the path where to search for the values (may be null)
   * @param property the name of the property
   * @param handler will be called when the values have been retrieved from the store
   */
  void getPropertyValues(String search, String path, String property,
    Handler<AsyncResult<AsyncCursor<String>>> handler);
}
