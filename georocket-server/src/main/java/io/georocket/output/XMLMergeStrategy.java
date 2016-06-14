package io.georocket.output;

import java.util.List;

import io.georocket.util.XMLStartElement;

/**
 * A merge strategy for XML chunks
 * @author Michel Kraemer
 */
public interface XMLMergeStrategy extends MergeStrategy {
  /**
   * Set XML parent elements for the chunks to merge
   * @param parents the parent elements
   */
  void setParents(List<XMLStartElement> parents);
  
  /**
   * @return the merged XML parent elements
   */
  List<XMLStartElement> getParents();
}
