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
   * Return that part of the Elasticsearch mapping returned by
   * {@link #getMapping()} that describes indexed attributes, or {@code null}
   * if the indexer does not collect indexed attributes. If the indexer only
   * collects indexed attributes and nothing else, this method can return the
   * same result as {@link #getMapping()}. The default implementation returns
   * {@code null}.
   * @return the part of the Elasticsearch mapping that describes indexed
   * attributes, or {@code null}
   * @since 1.4.0
   */
  default Map<String, Object> getIndexedAttributeMapping() {
    return null;
  }
  
  /**
   * @return a new instance of {@link Indexer}
   */
  Indexer createIndexer();
}
