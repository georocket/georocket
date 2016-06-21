package io.georocket.index.xml;

import io.georocket.index.generic.GmlIdIndexerFactory;

/**
 * Create instances of {@link GmlIdIndexer}
 * @author Michel Kraemer
 */
public class XMLGmlIdIndexerFactory extends GmlIdIndexerFactory implements XMLIndexerFactory {

  @Override
  public XMLIndexer createIndexer() {
    return new GmlIdIndexer();
  }
}
