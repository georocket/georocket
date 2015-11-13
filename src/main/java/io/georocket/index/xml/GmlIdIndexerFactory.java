package io.georocket.index.xml;

import io.georocket.api.index.xml.XMLIndexer;
import io.georocket.api.index.xml.XMLIndexerFactory;

/**
 * Create instances of {@link GmlIdIndexer}
 * @author Michel Kraemer
 */
public class GmlIdIndexerFactory implements XMLIndexerFactory {
  @Override
  public XMLIndexer createIndexer() {
    return new GmlIdIndexer();
  }
}
