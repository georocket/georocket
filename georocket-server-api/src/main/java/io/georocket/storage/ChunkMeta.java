package io.georocket.storage;

import java.util.List;
import java.util.stream.Collectors;

import io.georocket.util.XMLStartElement;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Metadata for a chunk
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class ChunkMeta {
  private final List<XMLStartElement> parents;
  private final int start;
  private final int end;
  
  /**
   * Create a new metadata object
   * @param parents the chunk's parents (i.e. the XML start elements the
   * chunk is wrapped in)
   * @param start the position in the stored blob where the chunk starts
   * (typically right after all its parent XML elements)
   * @param end the position in the stored blob where the chunk ends (typically
   * right before all its parent XML elements are closed)
   */
  public ChunkMeta(List<XMLStartElement> parents, int start, int end) {
    this.parents = parents;
    this.start = start;
    this.end = end;
  }
  
  /**
   * @return the chunk's parents (i.e. the XML start elements the
   * chunk is wrapped in)
   */
  public List<XMLStartElement> getParents() {
    return parents;
  }
  
  /**
   * @return the position in the stored blob where the chunk starts
   * (typically right after all its parent XML elements)
   */
  public int getStart() {
    return start;
  }
  
  /**
   * @return the position in the stored blob where the chunk ends (typically
   * right before all its parent XML elements are closed)
   */
  public int getEnd() {
    return end;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + end;
    result = prime * result + start;
    result = prime * result + ((parents == null) ? 0 : parents.hashCode());
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
    ChunkMeta other = (ChunkMeta) obj;
    if (end != other.end) {
      return false;
    }
    if (start != other.start) {
      return false;
    }
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
    return new JsonObject()
        .put("parents", ps)
        .put("start", start)
        .put("end", end);
  }
  
  /**
   * Converts a {@link JsonObject} to a {@link ChunkMeta} object
   * @param obj the {@link JsonObject} to convert
   * @return the {@link ChunkMeta} object
   */
  public static ChunkMeta fromJsonObject(JsonObject obj) {
    JsonArray parentsArr = obj.getJsonArray("parents");
    List<XMLStartElement> pl = parentsArr.stream().map(e ->
        XMLStartElement.fromJsonObject((JsonObject)e)).collect(Collectors.toList());
    return new ChunkMeta(pl, obj.getInteger("start"), obj.getInteger("end"));
  }
}
