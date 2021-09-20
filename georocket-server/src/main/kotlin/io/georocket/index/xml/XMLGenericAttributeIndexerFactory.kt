package io.georocket.index.xml

import io.georocket.index.generic.GenericAttributeIndexerFactory

/**
 * Create instances of [XMLGenericAttributeIndexer]
 * @author Michel Kraemer
 */
class XMLGenericAttributeIndexerFactory : GenericAttributeIndexerFactory(), XMLIndexerFactory {
  override fun createIndexer() = XMLGenericAttributeIndexer()
}
