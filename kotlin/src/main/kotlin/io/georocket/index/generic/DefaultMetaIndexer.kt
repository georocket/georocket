package io.georocket.index.generic

import io.georocket.index.MetaIndexer
import io.georocket.storage.IndexMeta

/**
 * Default implementation of an [MetaIndexer] that extracts generic
 * attributes from chunk metadata and adds it to the index.
 * @author Michel Kraemer
 */
class DefaultMetaIndexer : MetaIndexer {
  override fun onChunk(path: String, indexMeta: IndexMeta): Map<String, Any> {
    val result = mutableMapOf<String, Any>()

    result["path"] = path
    result["layer"] = indexMeta.layer

    if (indexMeta.tags != null) {
      result["tags"] = indexMeta.tags
    }

    if (indexMeta.properties != null) {
      result["props"] = indexMeta.properties.entries.map { e ->
        mapOf("key" to e.key, "value" to e.value)
      }
    }

    result["correlationId"] = indexMeta.correlationId

    return result
  }
}
