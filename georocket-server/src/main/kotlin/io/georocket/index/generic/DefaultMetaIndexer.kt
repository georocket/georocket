package io.georocket.index.generic

import io.georocket.index.xml.MetaIndexer
import io.georocket.storage.ChunkMeta
import io.georocket.storage.IndexMeta

/**
 * Default implementation of [MetaIndexer] that extracts generic
 * attributes from chunk metadata and adds it to the index.
 * @author Michel Kraemer
 */
class DefaultMetaIndexer : MetaIndexer {
  private val result = mutableMapOf<String, Any>()

  override fun getResult(): Map<String, Any> {
    return result
  }

  override fun onIndexChunk(path: String, chunkMeta: ChunkMeta, indexMeta: IndexMeta) {
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
  }
}
