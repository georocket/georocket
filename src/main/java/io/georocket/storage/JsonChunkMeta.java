package io.georocket.storage;

import io.vertx.core.json.JsonObject;

/**
 * Metadata for a JSON chunk
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class JsonChunkMeta extends ChunkMeta {
  /**
   * The mime type for JSON chunks
   */
  public static final String MIME_TYPE = "application/json";
  
  /**
   * The name of the field whose value is equal to this chunk or, if
   * the field is an array, whose value contains this chunk (may be
   * <code>null</code> if the chunk does not have a parent)
   */
  private final String parentFieldName;

  /**
   * Create a new metadata object
   * @param parentFieldName the name of the field whose value is equal to this
   * chunk or, if the field is an array, whose value contains this chunk (may
   * be <code>null</code> if the chunk does not have a parent)
   * @param start the position in the stored blob where the chunk starts
   * @param end the position in the stored blob where the chunk ends
   */
  public JsonChunkMeta(String parentFieldName, int start, int end) {
    super(start, end, MIME_TYPE);
    this.parentFieldName = parentFieldName;
  }
  
  /**
   * Create a new metadata object
   * @param parentFieldName the name of the field whose value is equal to this
   * chunk or, if the field is an array, whose value contains this chunk (may
   * be <code>null</code> if the chunk does not have a parent)
   * @param start the position in the stored blob where the chunk starts
   * @param end the position in the stored blob where the chunk ends
   * @param mimeType the chunk's mime type (should be "application/json" or a
   * subtype)
   */
  public JsonChunkMeta(String parentFieldName, int start, int end, String mimeType) {
    super(start, end, mimeType);
    this.parentFieldName = parentFieldName;
  }

  /**
   * Create a new metadata object from a JsonObject
   * @param json the JsonObject
   */
  public JsonChunkMeta(JsonObject json) {
    super(json);
    parentFieldName = json.getString("parentName");
  }
  
  /**
   * @return the name of the field whose value is equal to this chunk or, if
   * the field is an array, whose value contains this chunk (may be
   * <code>null</code> if the chunk does not have a parent)
   */
  public String getParentFieldName() {
    return parentFieldName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((parentFieldName == null) ? 0 : parentFieldName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    JsonChunkMeta other = (JsonChunkMeta)obj;
    if (parentFieldName == null) {
      if (other.parentFieldName != null) {
        return false;
      }
    } else if (!parentFieldName.equals(other.parentFieldName)) {
      return false;
    }
    return true;
  }
  
  /**
   * @return this object as a {@link JsonObject}
   */
  public JsonObject toJsonObject() {
    JsonObject r = super.toJsonObject();
    if (parentFieldName != null) {
      r.put("parentName", parentFieldName);
    }
    return r;
  }
}
