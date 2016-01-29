package io.georocket.api.index;

import java.util.Map;

import io.georocket.api.query.QueryCompiler;

/**
 * Factory for {@link Indexer} objects
 * @author Michel Kraemer
 */
public interface IndexerFactory extends QueryCompiler {
  /**
   * @return the Elasticsearch mapping for the results returned by the indexer
   */
  Map<String, Object> getMapping();
  
  /**
   * @return a new instance of {@link Indexer}
   */
  Indexer createIndexer();
}
