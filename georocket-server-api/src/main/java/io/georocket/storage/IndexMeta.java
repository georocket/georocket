package io.georocket.storage;

import java.util.Date;
import java.util.List;

/**
 * Metadata affecting the way a chunk is indexed
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class IndexMeta {
  private final List<String> tags;
  private final String fallbackCRSString;
  private final String correlationId;
  private final String fromFile;
  private final Date importTimeStamp;

  /**
   * Default constructor
   * @param correlationId identifies to which specific import this meta data belongs
   * @param fromFile the name of the source file containing the chunks to be indexed
   * @param importTimeStamp The timestamp for this import
   */
  public IndexMeta(String correlationId, String fromFile, Date importTimeStamp) {
    this(correlationId, fromFile, importTimeStamp, null, null);
  }
  
  /**
   * Construct the parameters
   * @param correlationId identifies to which specific import this meta data belongs
   * @param fromFile the name of the source file containing the chunks to be indexed
   * @param importTimeStamp the timestamp for this import
   * @param tags the list of tags to attach to the chunk (may be null)
   */
  public IndexMeta(String correlationId, String fromFile, Date importTimeStamp,
      List<String> tags) {
    this(correlationId, fromFile, importTimeStamp, tags, null);
  }
  
  /**
   * Construct the parameters
   * @param correlationId identifies to which specific import this meta data belongs
   * @param fromFile the name of the source file containing the chunks to be indexed
   * @param importTimeStamp the timestamp for this import
   * @param tags the list of tags to attach to the chunk (may be null)
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk to import if it does not specify a CRS itself (may be
   * null if no CRS is available as fallback)
   */
  public IndexMeta(String correlationId, String fromFile, Date importTimeStamp,
      List<String> tags, String fallbackCRSString) {
    this.correlationId = correlationId;
    this.fromFile = fromFile;
    this.importTimeStamp = importTimeStamp;
    this.tags = tags;
    this.fallbackCRSString = fallbackCRSString;
  }

  /**
   * @return an ID that specifies to which specific import this meta data belongs
   */
  public String getCorrelationId() {
    return this.correlationId;
  }

  /**
   * @return the name of the source file containing the chunks to be indexed
   */
  public String getFromFile() {
    return this.fromFile;
  }

  /**
   * @return the timestamp for this import
   */
  public Date getImportTimeStamp() {
    return this.importTimeStamp;
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
