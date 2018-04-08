package io.georocket.output.xml;

import java.util.List;

import io.georocket.output.Merger;
import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.XMLStartElement;
import rx.Single;

/**
 * A merge strategy for XML chunks
 * @author Michel Kraemer
 */
public interface MergeStrategy extends Merger<XMLChunkMeta> {
  /**
   * Set XML parent elements for the chunks to merge
   * @param parents the parent elements
   */
  void setParents(List<XMLStartElement> parents);
  
  /**
   * @return the merged XML parent elements
   */
  List<XMLStartElement> getParents();
  
  /**
   * Check if a chunk with the given metadata can be merged and call a
   * handler with the result
   * @param meta the chunk metadata
   * @return a Single that will emit <code>true</code> if the metadata
   * can be merged and <code>false</code> otherwise
   */
  Single<Boolean> canMerge(XMLChunkMeta meta);
}
