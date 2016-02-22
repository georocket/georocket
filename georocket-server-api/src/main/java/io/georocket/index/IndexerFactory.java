package io.georocket.index;

import java.util.Map;

import io.georocket.query.QueryCompiler;

/**
 * Factory for {@link Indexer} objects
 * @since 1.0.0
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
