package io.georocket.index.generic

import io.georocket.index.MetaIndexer
import io.georocket.storage.ChunkMeta
import io.georocket.storage.IndexMeta

/**
 * Default implementation of an [MetaIndexer] that extracts generic
 * attributes from chunk metadata and adds it to the index.
 * @author Michel Kraemer
 */
class DefaultMetaIndexer : MetaIndexer {
  override fun onChunk(path: String, chunkMeta: ChunkMeta, indexMeta: IndexMeta): Map<String, Any> {
    val result = mutableMapOf<String, Any>()

    result["path"] = path
    result["chunkMeta"] = chunkMeta.toJsonObject()

    if (indexMeta.tags != null) {
      result["tags"] = indexMeta.tags
    }

    if (indexMeta.properties != null) {
      result["props"] = indexMeta.properties
    }

    if (indexMeta.correlationId != null) {
      result["correlationId"] = indexMeta.correlationId
    }

    return result
  }
}
