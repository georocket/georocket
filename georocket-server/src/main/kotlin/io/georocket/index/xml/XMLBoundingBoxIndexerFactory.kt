package io.georocket.index.xml

import io.georocket.index.generic.BoundingBoxIndexerFactory

/**
 * Create instances of [XMLBoundingBoxIndexer]
 * @author Michel Kraemer
 */
class XMLBoundingBoxIndexerFactory : BoundingBoxIndexerFactory(), XMLIndexerFactory {
  override fun createIndexer() = XMLBoundingBoxIndexer()
}
