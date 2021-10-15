package io.georocket.index

import io.georocket.query.QueryCompiler
import io.georocket.util.StreamEvent

/**
 * Factory for [Indexer] objects
 * @author Michel Kraemer
 */
interface IndexerFactory : QueryCompiler {
  /**
   * Returns a new instance of [Indexer] for the given [eventType]. Returns
   * `null` if the type is not supported.
   */
  fun <T : StreamEvent> createIndexer(eventType: Class<T>): Indexer<T>?
}
