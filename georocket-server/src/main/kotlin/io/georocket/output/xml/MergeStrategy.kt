package io.georocket.output.xml

import io.georocket.output.Merger
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.XMLStartElement
import rx.Single

/**
 * A merge strategy for XML chunks
 * @author Michel Kraemer
 */
interface MergeStrategy : Merger<XMLChunkMeta> {
  /**
   * The XML parent elements
   */
  var parents: List<XMLStartElement>?

  /**
   * Check if a chunk with the given metadata can be merged and call a
   * handler with the result
   * @param meta the chunk metadata
   * @return a Single that will emit `true` if the metadata
   * can be merged and `false` otherwise
   */
  fun canMerge(meta: XMLChunkMeta): Single<Boolean>
}
