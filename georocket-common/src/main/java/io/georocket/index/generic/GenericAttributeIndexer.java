package io.georocket.index.generic;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import io.georocket.index.Indexer;

/**
 * Base class for all indexers that find generic attributes (i.e. properties,
 * key-value pairs) in chunks
 * @author Michel Kraemer
 */
public class GenericAttributeIndexer implements Indexer {
  /**
   * Map collecting all attributes parsed
   */
  private Map<String, String> result = new HashMap<>();
  
  protected void put(String key, String value) {
    // Remove dot from field name so that Elasticsearch does not think
    // this field is of type 'object' instead of 'keyword'
    key = key.replace('.', '_');
    
    result.put(key, value);
  }
  
  @Override
  public Map<String, Object> getResult() {
    return ImmutableMap.of("genAttrs", result);
  }
}
