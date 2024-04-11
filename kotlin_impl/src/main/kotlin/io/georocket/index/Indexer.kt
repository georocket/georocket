package io.georocket.index

import io.georocket.util.StreamEvent

/**
 * Indexes chunks
 * @author Michel Kraemer
 */
interface Indexer<T : StreamEvent> {
  /**
   * Will be called on every stream [event] in the chunk
   */
  fun onEvent(event: T)

  /**
   * Will be called when the whole chunk has been passed to the indexer.
   * Returns the results that should be added to the index or an empty
   * map if nothing should be added.
   */
  fun makeResult(): Map<String, Any>
}
