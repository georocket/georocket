package io.georocket.index

import io.georocket.query.QueryCompiler

/**
 * Factory for [MetaIndexer] objects
 * @author Michel Kraemer
 */
interface MetaIndexerFactory : QueryCompiler {
  /**
   * Returns a new instance of [MetaIndexer]
   */
  fun createIndexer(): MetaIndexer
}
