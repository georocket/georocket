package io.georocket.index;

import io.georocket.query.QueryCompiler;

/**
 * Factory for {@link Indexer} objects
 * @since 1.0.0
 * @author Michel Kraemer
 */
public interface IndexerFactory extends QueryCompiler {
  /**
   * @return a new instance of {@link Indexer}
   */
  Indexer createIndexer();
}
