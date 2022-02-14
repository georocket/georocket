package io.georocket.index

import io.georocket.index.generic.BoundingBoxIndexerFactory
import io.georocket.index.generic.GenericAttributeIndexerFactory
import io.georocket.index.geojson.GeoJsonIdIndexerFactory
import io.georocket.index.xml.GmlIdIndexerFactory
import io.georocket.index.xml.XalAddressIndexerFactory
import io.georocket.query.QueryCompiler
import io.georocket.util.StreamEvent

/**
 * Factory for [Indexer] objects
 * @author Michel Kraemer
 */
interface IndexerFactory : QueryCompiler {
  companion object {
    val ALL = listOf(
      BoundingBoxIndexerFactory(),
      GenericAttributeIndexerFactory(),
      GmlIdIndexerFactory(),
      GeoJsonIdIndexerFactory(),
      XalAddressIndexerFactory()
    )
  }

  /**
   * Returns a new instance of [Indexer] for the given [eventType]. Returns
   * `null` if the type is not supported.
   */
  fun <T : StreamEvent> createIndexer(eventType: Class<T>): Indexer<T>?
}
