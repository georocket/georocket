package io.georocket.index.xml;

import io.georocket.index.generic.GenericAttributeIndexerFactory;

/**
 * Create instances of {@link GenericAttributeIndexer}
 * @author Michel Kraemer
 */
public class XMLGenericAttributeIndexerFactory extends GenericAttributeIndexerFactory implements XMLIndexerFactory {
  
  @Override
  public XMLIndexer createIndexer() {
    return new GenericAttributeIndexer();
  }

}
