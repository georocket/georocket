package io.georocket.index

import io.georocket.storage.IndexMeta

/**
 * Indexes metadata of chunks
 * @author Michel Kraemer
 */
interface MetaIndexer {
  /**
   * Will be called when a chunk with the given [path] and [indexMeta] is indexed
   */
  fun onChunk(path: String, indexMeta: IndexMeta): Map<String, Any>
}
