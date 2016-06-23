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
  private final String importId;
  private final String fromFile;
  private final Date importTimeStamp;

  /**
   * Default constructor
   * @param importId The import id - identify to which specific import this meta data belongs
   * @param fromFile The name of the imported file
   * @param importTimeStamp The timestamp for this import
   */
  public IndexMeta(String importId, String fromFile, Date importTimeStamp) {
    this(importId, fromFile, importTimeStamp, null, null);
  }
  
  /**
   * Construct the parameters
   * @param importId The import id - identify to which specific import this meta data belongs
   * @param fromFile The name of the imported file
   * @param importTimeStamp The timestamp for this import
   * @param tags the list of tags to attach to the chunk (may be null)
   */
  public IndexMeta(String importId, String fromFile, Date importTimeStamp, List<String> tags) {
    this(importId, fromFile, importTimeStamp, tags, null);
  }
  
  /**
   * Construct the parameters
   * @param importId The import id - identify to which specific import this meta data belongs
   * @param fromFile The name of the imported file
   * @param importTimeStamp The timestamp for this import
   * @param tags the list of tags to attach to the chunk (may be null)
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk to import if it does not specify a CRS itself (may be
   * null if no CRS is available as fallback)
   */
  public IndexMeta(String importId, String fromFile, Date importTimeStamp, List<String> tags, String fallbackCRSString) {
    this.importId = importId;
    this.fromFile = fromFile;
    this.importTimeStamp = importTimeStamp;
    this.tags = tags;
    this.fallbackCRSString = fallbackCRSString;
  }

  /**
   * @return the id for the import where this meta data was created from
   */
  public String getImportId() {
    return this.importId;
  }

  /**
   * @return the file name of the file this meta data was created from
   */
  public String getFromFile() {
    return this.fromFile;
  }

  /**
   * @return the timestamp of the event the data was imported
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
