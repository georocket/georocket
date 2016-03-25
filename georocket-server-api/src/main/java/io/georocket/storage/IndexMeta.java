package io.georocket.storage;

import java.util.List;

/**
 * Metadata affecting the way a chunk is indexed
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class IndexMeta {
  private final List<String> tags;
  private final String fallbackCRSString;
  
  /**
   * Default constructor
   */
  public IndexMeta() {
    this(null, null);
  }
  
  /**
   * Construct the parameters
   * @param tags the list of tags to attach to the chunk (may be null)
   */
  public IndexMeta(List<String> tags) {
    this(tags, null);
  }
  
  /**
   * Construct the parameters
   * @param tags the list of tags to attach to the chunk (may be null)
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk to import if it does not specify a CRS itself (may be
   * null if no CRS is available as fallback)
   */
  public IndexMeta(List<String> tags, String fallbackCRSString) {
    this.tags = tags;
    this.fallbackCRSString = fallbackCRSString;
  }
  
  /**
   * @return the list of tags to attach to the chunk (may be null)
   */
  public List<String> getTags() {
    return tags;
  }
  
  /**
   * @return a string representing the CRS that should be used to index the
   * chunk to import if it does not specify a CRS itself (may be null if no
   * CRS is available as fallback)
   */
  public String getFallbackCRSString() {
    return fallbackCRSString;
  }
}
