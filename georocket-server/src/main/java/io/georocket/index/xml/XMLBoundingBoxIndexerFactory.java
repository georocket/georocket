package io.georocket.index.xml;

import io.georocket.index.generic.BoundingBoxIndexerFactory;

/**
 * Create instances of {@link BoundingBoxIndexer}
 * @author Michel Kraemer
 */
public class XMLBoundingBoxIndexerFactory extends BoundingBoxIndexerFactory implements XMLIndexerFactory {
  
  @Override
  public XMLIndexer createIndexer() {
    return new BoundingBoxIndexer();
  }
  
}
