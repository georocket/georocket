package io.georocket.index.xml;

import io.georocket.api.index.xml.XMLIndexer;
import io.georocket.api.index.xml.XMLIndexerFactory;

/**
 * Create instances of {@link BoundingBoxIndexer}
 * @author Michel Kraemer
 */
public class BoundingBoxIndexerFactory implements XMLIndexerFactory {
  @Override
  public XMLIndexer createIndexer() {
    return new BoundingBoxIndexer();
  }
}
