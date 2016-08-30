package io.georocket.storage;

import java.util.List;
import java.util.stream.Collectors;

import io.georocket.util.XMLStartElement;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Metadata for an XML chunk
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class XMLChunkMeta extends ChunkMeta {
  /**
   * The mime type for XML chunks
   */
  public static final String MIME_TYPE = "application/xml";

  private final List<XMLStartElement> parents;
  
  /**
   * Create a new metadata object
   * @param parents the chunk's parents (i.e. the XML start elements the
   * chunk is wrapped in)
   * @param start the position in the stored blob where the chunk starts
   * (typically right after all its parent XML elements)
   * @param end the position in the stored blob where the chunk ends (typically
   * right before all its parent XML elements are closed)
   */
  public XMLChunkMeta(List<XMLStartElement> parents, int start, int end) {
    super(start, end, MIME_TYPE);
    this.parents = parents;
  }
  
  /**
   * Create a new metadata object from a JsonObject
   * @param json the JsonObject
   */
  public XMLChunkMeta(JsonObject json) {
    super(json);
    this.parents = json.getJsonArray("parents").stream()
        .map(e -> XMLStartElement.fromJsonObject((JsonObject)e))
        .collect(Collectors.toList());
  }
  
  /**
   * @return the chunk's parents (i.e. the XML start elements the
   * chunk is wrapped in)
   */
  public List<XMLStartElement> getParents() {
    return parents;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((parents == null) ? 0 : parents.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    XMLChunkMeta other = (XMLChunkMeta)obj;
    if (parents == null) {
      if (other.parents != null) {
        return false;
      }
    } else if (!parents.equals(other.parents)) {
      return false;
    }
    return true;
  }
  
  /**
   * @return this object as a {@link JsonObject}
   */
  public JsonObject toJsonObject() {
    JsonArray ps = new JsonArray();
    parents.forEach(p -> ps.add(p.toJsonObject()));
    return super.toJsonObject()
        .put("parents", ps);
  }
}
