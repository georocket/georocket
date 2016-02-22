package io.georocket.index.xml;

import io.georocket.index.IndexerFactory;

/**
 * Factory for {@link XMLIndexer} objects
 * @since 1.0.0
 * @author Michel Kraemer
 */
public interface XMLIndexerFactory extends IndexerFactory {
  /**
   * @return a new instance of {@link XMLIndexer}
   */
  @Override
  XMLIndexer createIndexer();
}
