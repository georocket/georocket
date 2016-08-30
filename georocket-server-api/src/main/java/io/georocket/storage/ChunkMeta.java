package io.georocket.storage;

import io.vertx.core.json.JsonObject;

/**
 * Metadata for a chunk
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class ChunkMeta {
  protected final int start;
  protected final int end;
  protected final String mimeType;

  /**
   * Create a new metadata object
   * @param start the position in the stored blob where the chunk starts
   * @param end the position in the stored blob where the chunk ends
   * @param mimeType the chunk's mime type (typically "application/xml" or
   * "application/json")
   */
  public ChunkMeta(int start, int end, String mimeType) {
    this.start = start;
    this.end = end;
    this.mimeType = mimeType;
  }
  
  /**
   * Create a new metadata object from a JsonObject
   * @param json the JsonObject
   */
  public ChunkMeta(JsonObject json) {
    this(json.getInteger("start"), json.getInteger("end"),
        json.getString("mimeType", "application/xml"));
  }

  /**
   * @return the position in the stored blob where the chunk starts
   */
  public int getStart() {
    return start;
  }

  /**
   * @return the position in the stored blob where the chunk ends
   */
  public int getEnd() {
    return end;
  }

  /**
   * @return the chunk's mime type (typically "application/xml" or
   * "application/json")
   */
  public String getMimeType() {
    return mimeType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + end;
    result = prime * result + start;
    result = prime * result + ((mimeType == null) ? 0 : mimeType.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ChunkMeta other = (ChunkMeta)obj;
    if (end != other.end) {
      return false;
    }
    if (start != other.start) {
      return false;
    }
    if (mimeType == null) {
      if (other.mimeType != null) {
        return false;
      }
    } else if (!mimeType.equals(other.mimeType)) {
      return false;
    }
    return true;
  }

  /**
   * @return this object as a {@link JsonObject}
   */
  public JsonObject toJsonObject() {
    return new JsonObject()
        .put("start", start)
        .put("end", end)
        .put("mimeType", mimeType);
  }
}
