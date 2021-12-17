package io.georocket.output.xml

import io.georocket.storage.XMLChunkMeta

/**
 * Merge chunks whose root XML elements are all equal
 * @author Michel Kraemer
 */
class AllSameStrategy : AbstractMergeStrategy() {
  override fun canMerge(metadata: XMLChunkMeta): Boolean {
    return (parents == null || parents == metadata.parents)
  }

  override fun mergeParents(chunkMetadata: XMLChunkMeta) {
    if (parents == null) {
      parents = chunkMetadata.parents
    }
  }
}
