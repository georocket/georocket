package io.georocket.output.xml

import io.georocket.storage.XmlChunkMeta

/**
 * Merge chunks whose root XML elements are all equal
 * @author Michel Kraemer
 */
class AllSameStrategy : AbstractMergeStrategy() {
  override fun canMerge(metadata: XmlChunkMeta): Boolean {
    return (parents == null || parents == metadata.parents)
  }

  override fun mergeParents(chunkMetadata: XmlChunkMeta) {
    if (parents == null) {
      parents = chunkMetadata.parents
    }
  }
}
