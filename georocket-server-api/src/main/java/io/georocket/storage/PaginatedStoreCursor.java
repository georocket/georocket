package io.georocket.storage;

import io.vertx.core.json.JsonObject;

/**
 * A cursor that can be used to iterate over chunks in a {@link Store}
 * and return paginated results.
 * @since 1.1.0
 * @author David Gengenbach
 */
public interface PaginatedStoreCursor extends StoreCursor {
  /**
   * @return pagination info about the current batch of items
   */
  JsonObject getPaginationInfo();
}
