package io.georocket.index.generic;

import java.util.HashMap;
import java.util.Map;

import io.georocket.index.xml.MetaIndexer;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.IndexMeta;

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
    result.put("chunkMeta", chunkMeta.toJsonObject());
    if (indexMeta.getTags() != null) {
      result.put("tags", indexMeta.getTags());
    }
    if (indexMeta.getProperties() != null) {
      result.put("props", indexMeta.getProperties());
    }
    if (indexMeta.getCorrelationId() != null) {
      result.put("correlationId", indexMeta.getCorrelationId());
    }
  }
}
