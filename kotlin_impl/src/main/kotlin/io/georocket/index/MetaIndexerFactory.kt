package io.georocket.index

import io.georocket.index.generic.DefaultMetaIndexerFactory
import io.georocket.query.QueryCompiler

/**
 * Factory for [MetaIndexer] objects
 * @author Michel Kraemer
 */
interface MetaIndexerFactory : QueryCompiler {
  companion object {
    val ALL = listOf<MetaIndexerFactory>(
      DefaultMetaIndexerFactory()
    )
  }

  /**
   * Returns a new instance of [MetaIndexer]
   */
  fun createIndexer(): MetaIndexer
}
