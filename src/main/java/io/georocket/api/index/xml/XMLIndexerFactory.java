package io.georocket.api.index.xml;

/**
 * Factory for {@link XMLIndexer} objects
 * @author Michel Kraemer
 */
public interface XMLIndexerFactory {
  /**
   * @return a new instance of {@link XMLIndexer}
   */
  XMLIndexer createIndexer();
}
