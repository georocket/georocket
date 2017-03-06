package io.georocket.storage;

import java.util.List;
import java.util.Map;

/**
 * Metadata affecting the way a chunk is indexed
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class IndexMeta {
  private final List<String> tags;
  private final Map<String, Object> properties;
  private final String fallbackCRSString;
  private final String correlationId;
  private final String filename;
  private final long timestamp;

  /**
   * Default constructor
   * @param correlationId identifies to which specific import this meta data belongs
   * @param filename the name of the source file containing the chunks to be indexed
   * @param timestamp the timestamp for this import
   */
  public IndexMeta(String correlationId, String filename, long timestamp) {
    this(correlationId, filename, timestamp, null, null, null);
  }
  
  /**
   * Construct the parameters
   * @param correlationId identifies to which specific import this meta data belongs
   * @param filename the name of the source file containing the chunks to be indexed
   * @param timestamp the timestamp for this import
   * @param tags the list of tags to attach to the chunk (may be null)
   * @param properties the map of properties to attach to the chunk (may be null)
   */
  public IndexMeta(String correlationId, String filename, long timestamp,
      List<String> tags, Map<String, Object> properties) {
    this(correlationId, filename, timestamp, tags, properties, null);
  }
  
  /**
   * Construct the parameters
   * @param correlationId identifies to which specific import this meta data belongs
   * @param filename the name of the source file containing the chunks to be indexed
   * @param timestamp the timestamp for this import
   * @param tags the list of tags to attach to the chunk (may be null)
   * @param properties the map of properties to attach to the chunk (may be null)
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk to import if it does not specify a CRS itself (may be
   * null if no CRS is available as fallback)
   */
  public IndexMeta(String correlationId, String filename, long timestamp,
      List<String> tags, Map<String, Object> properties, String fallbackCRSString) {
    this.correlationId = correlationId;
    this.filename = filename;
    this.timestamp = timestamp;
    this.tags = tags;
    this.properties = properties;
    this.fallbackCRSString = fallbackCRSString;
  }

  /**
   * @return an ID that specifies to which specific import this meta data belongs
   */
  public String getCorrelationId() {
    return correlationId;
  }

  /**
   * @return the name of the source file containing the chunks to be indexed
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @return the timestamp for this import
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return the list of tags to attach to the chunk (may be null)
   */
  public List<String> getTags() {
    return tags;
  }

  /**
   * @return the map of properties to attach to the chunk (may be null)
   */
  public Map<String, Object> getProperties() {
    return properties;
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
