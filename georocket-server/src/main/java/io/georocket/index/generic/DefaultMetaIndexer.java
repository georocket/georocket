package io.georocket.index.generic;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import io.georocket.index.xml.MetaIndexer;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.IndexMeta;
import io.vertx.core.json.JsonObject;

/**
 * Default implementation of {@link MetaIndexer} that extracts generic
 * attributes from chunk metadata and adds it to the index.
 * @author Michel Kraemer
 */
public class DefaultMetaIndexer implements MetaIndexer {
  private final Map<String, Object> result = new HashMap<>();
  
  @Override
  public Map<String, Object> getResult() {
    return result;
  }

  @Override
  public void onIndexChunk(String path, ChunkMeta chunkMeta,
      IndexMeta indexMeta) {
    result.put("path", path);
    result.put("correlationId", indexMeta.getCorrelationId());
    result.put("filename", indexMeta.getFilename());
    result.put("timestamp", indexMeta.getTimestamp());
    result.putAll(chunkMetaToDocument(chunkMeta));
    if (indexMeta.getTags() != null) {
      result.put("tags", indexMeta.getTags());
    }
  }

  /**
   * Convert chunk metadata to an Elasticsearch document. Copy all properties
   * but prepend the string "chunk" to all property names and convert the first
   * character to upper case (or insert an underscore character if it already
   * is upper case).
   * @param chunkMeta the metadata to convert
   * @return the converted document
   */
  public static Map<String, Object> chunkMetaToDocument(ChunkMeta chunkMeta) {
    Map<String, Object> result = new HashMap<>();
    JsonObject meta = chunkMeta.toJsonObject();
    for (String fieldName : meta.fieldNames()) {
      String newFieldName = fieldName;
      if (Character.isTitleCase(newFieldName.charAt(0))) {
        newFieldName = "_" + newFieldName;
      } else {
        newFieldName = StringUtils.capitalize(newFieldName);
      }
      newFieldName = "chunk" + newFieldName;
      result.put(newFieldName, meta.getValue(fieldName));
    }
    return result;
  }

  /**
   * Convert an Elasticsearch document to a chunk metadata object. This
   * method returns a new JsonObject that can be passed to a constructor
   * of one of the {@link ChunkMeta} implementations.
   * @param source the document to convert
   * @return the chunk metadata
   */
  public static JsonObject documentToChunkMeta(JsonObject source) {
    JsonObject filteredSource = new JsonObject();
    for (String fieldName : source.fieldNames()) {
      if (fieldName.startsWith("chunk")) {
        String newFieldName = fieldName.substring(5);
        if (newFieldName.charAt(0) == '_') {
          newFieldName = newFieldName.substring(1);
        } else {
          newFieldName = StringUtils.uncapitalize(newFieldName);
        }
        filteredSource.put(newFieldName, source.getValue(fieldName));
      }
    }
    return filteredSource;
  }
}
