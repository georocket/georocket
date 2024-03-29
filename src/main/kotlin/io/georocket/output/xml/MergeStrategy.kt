package io.georocket.output.xml

import io.georocket.output.Merger
import io.georocket.storage.XmlChunkMeta
import io.georocket.util.XMLStartElement

/**
 * A merge strategy for XML chunks
 * @author Michel Kraemer
 */
interface MergeStrategy : Merger<XmlChunkMeta> {
  /**
   * The XML parent elements
   */
  var parents: List<XMLStartElement>?

  /**
   * Check if a chunk with the given [metadata] can be merged
   */
  fun canMerge(metadata: XmlChunkMeta): Boolean
}
