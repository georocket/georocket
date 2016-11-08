package io.georocket.index.xml;

import io.georocket.index.IndexerFactory;

/**
 * Factory for {@link JsonIndexer} objects
 * @since 1.0.0
 * @author Michel Kraemer
 */
public interface JsonIndexerFactory extends IndexerFactory {
  /**
   * @return a new instance of {@link JsonIndexer}
   */
  @Override
  JsonIndexer createIndexer();
}
