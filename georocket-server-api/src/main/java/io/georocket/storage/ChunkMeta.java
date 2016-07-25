package io.georocket.storage;

import java.util.List;
import java.util.Map;
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
   * Chunk Meta from JsonObject
   * @param object The JsonObject containing the values for the ChunkMeta Object.
   */
  public ChunkMeta(JsonObject object){
    JsonArray parentsArr = object.getJsonArray("parents");
    parents = parentsArr.stream().map(e ->
        XMLStartElement.fromJsonObject((JsonObject)e)).collect(Collectors.toList());
    start = object.getInteger("start");
    end = object.getInteger("end");
  }
  
  /**
   * Chunk Meta from Map.
   * Used to convert Elasticsearch hit to ChunkMeta
   * @param source The Elasticsearch document
   */
  @SuppressWarnings("unchecked")
  public ChunkMeta(Map<String, Object> source){
    start = ((Number)source.get("chunkStart")).intValue();
    end = ((Number)source.get("chunkEnd")).intValue();
    
    List<Map<String, Object>> parentsList = (List<Map<String, Object>>)source.get("chunkParents");
    parents = parentsList.stream().map(p -> {
      String prefix = (String)p.get("prefix");
      String localName = (String)p.get("localName");
      String[] namespacePrefixes = safeListToArray((List<String>)p.get("namespacePrefixes"));
      String[] namespaceUris = safeListToArray((List<String>)p.get("namespaceUris"));
      String[] attributePrefixes = safeListToArray((List<String>)p.get("attributePrefixes"));
      String[] attributeLocalNames = safeListToArray((List<String>)p.get("attributeLocalNames"));
      String[] attributeValues = safeListToArray((List<String>)p.get("attributeValues"));
      return new XMLStartElement(prefix, localName, namespacePrefixes, namespaceUris,
          attributePrefixes, attributeLocalNames, attributeValues);
    }).collect(Collectors.toList());
  }
  
  /**
   * Convert a list to an array. If the list is null the return value
   * will also be null.
   * @param list the list to convert
   * @return the array or null if <code>list</code> is null
   */
  private String[] safeListToArray(List<String> list) {
    if (list == null) {
      return null;
    }
    return list.toArray(new String[list.size()]);
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
        .put("end", end)
        .put("$type", ChunkMeta.class.getName());
  }
  
  /**
   * Converts a {@link JsonObject} to a {@link ChunkMeta} object
   * @param obj the {@link JsonObject} to convert
   * @return the {@link ChunkMeta} object
   */
  public static ChunkMeta fromJsonObject(JsonObject obj) {
    return new ChunkMeta(obj);
  }
  
  /**
   * Add own meta data to the given document
   * @param doc The document to add the meta to
   */
  public void addMeta(Map<String, Object> doc) {
    doc.put("chunkStart", start);
    doc.put("chunkEnd", end);
    doc.put("chunkParents", parents.stream().map(p ->
        p.toJsonObject().getMap()).collect(Collectors.toList()));
    doc.put("$type", this.getClass().getName());
  }
}
