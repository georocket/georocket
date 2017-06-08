package io.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;
import java.util.Map;

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
   * @param tag the list of tags to remove
   * @param handler will be called when the tags are removed
   */
  void removeTags(String search, String path, List<String> tag,
    Handler<AsyncResult<Void>> handler);
}
