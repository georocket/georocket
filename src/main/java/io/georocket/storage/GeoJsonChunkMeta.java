package io.georocket.storage;

import io.vertx.core.json.JsonObject;

/**
 * Metadata for a GeoJSON chunk
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class GeoJsonChunkMeta extends JsonChunkMeta {
  /**
   * The mime type for GeoJSON chunks
   */
  public static final String MIME_TYPE = "application/geo+json";
  
  /**
   * The type of the GeoJSON object represented by the chunk
   */
  private final String type;

  /**
   * Create a new metadata object
   * @param type the type of the GeoJSON object represented by the chunk
   * @param parentFieldName the name of the field whose value is equal to this
   * chunk or, if the field is an array, whose value contains this chunk (may
   * be <code>null</code> if the chunk does not have a parent)
   * @param start the position in the stored blob where the chunk starts
   * @param end the position in the stored blob where the chunk ends
   */
  public GeoJsonChunkMeta(String type, String parentFieldName, int start, int end) {
    super(parentFieldName, start, end, MIME_TYPE);
    this.type = type;
  }

  /**
   * Create a new metadata object from a JsonObject
   * @param json the JsonObject
   */
  public GeoJsonChunkMeta(JsonObject json) {
    super(json);
    type = json.getString("type");
  }
  
  /**
   * Makes a copy of the given {@link JsonChunkMeta} object but assigns a
   * GeoJSON object type
   * @param type the type of the GeoJSON object represented by the chunk
   * @param chunkMeta the chunk metadata object to copy
   */
  public GeoJsonChunkMeta(String type, JsonChunkMeta chunkMeta) {
    super(chunkMeta.getParentFieldName(), chunkMeta.getStart(),
      chunkMeta.getEnd(), MIME_TYPE);
    this.type = type;
  }
  
  /**
   * @return the type of the GeoJSON object represented by the chunk
   */
  public String getType() {
    return type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((type == null) ? 0 : type.hashCode());
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
    GeoJsonChunkMeta other = (GeoJsonChunkMeta)obj;
    if (type == null) {
      if (other.type != null) {
        return false;
      }
    } else if (!type.equals(other.type)) {
      return false;
    }
    return true;
  }

  /**
   * @return this object as a {@link JsonObject}
   */
  public JsonObject toJsonObject() {
    JsonObject r = super.toJsonObject();
    if (type != null) {
      r.put("type", type);
    }
    return r;
  }
}
