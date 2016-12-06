package io.georocket.index.xml;

import io.georocket.index.IndexerFactory;

/**
 * Factory for {@link MetaIndexer} objects
 * @since 1.0.0
 * @author Michel Kraemer
 */
public interface MetaIndexerFactory extends IndexerFactory {
  /**
   * @return a new instance of {@link MetaIndexer}
   */
  @Override
  MetaIndexer createIndexer();
}
