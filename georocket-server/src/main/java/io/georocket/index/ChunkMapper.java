package io.georocket.index;

import java.util.Map;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.vertx.core.json.JsonObject;
import rx.Observable;

/**
 * Maps chunks to Elasticsearch documents and vice-versa
 * @author Michel Kraemer
 */
public interface ChunkMapper {
  /**
   * Create a {@link ChunkMeta} object from the given JSON object
   * @param json the JSON object
   * @return the {@link ChunkMeta} object
   */
  ChunkMeta makeChunkMeta(JsonObject json);
  
  /**
   * Convert a chunk to a Elasticsearch document
   * @param chunk the chunk to convert
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk if it does not specify a CRS itself (may be null if no
   * CRS is available as fallback)
   * @return an observable that will emit the document
   */
  Observable<Map<String, Object>> chunkToDocument(ChunkReadStream chunk,
      String fallbackCRSString);
}
