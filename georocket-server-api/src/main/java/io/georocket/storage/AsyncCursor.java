package io.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * A cursor that can be used to iterate asynchronously over items
 * @author Tim Hellhake
 * @since 1.1.0
 * @param <T> type of the item
 */
public interface AsyncCursor<T> {
  /**
   * @return true if there are more items to iterate over
   */
  boolean hasNext();

  /**
   * Get the next item
   * @param handler will be called when the item has been retrieved
   */
  void next(Handler<AsyncResult<T>> handler);
}
