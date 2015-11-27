package io.georocket.api.index.xml;

import io.georocket.api.index.IndexerFactory;

/**
 * Factory for {@link XMLIndexer} objects
 * @author Michel Kraemer
 */
public interface XMLIndexerFactory extends IndexerFactory {
  /**
   * @return a new instance of {@link XMLIndexer}
   */
  @Override
  XMLIndexer createIndexer();
}
