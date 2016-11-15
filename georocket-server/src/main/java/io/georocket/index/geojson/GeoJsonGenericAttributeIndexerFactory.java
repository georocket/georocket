package io.georocket.index.geojson;

import io.georocket.index.generic.GenericAttributeIndexerFactory;
import io.georocket.index.xml.JsonIndexer;
import io.georocket.index.xml.JsonIndexerFactory;

/**
 * Create instances of {@link GeoJsonGenericAttributeIndexer}
 * @author Michel Kraemer
 */
public class GeoJsonGenericAttributeIndexerFactory extends GenericAttributeIndexerFactory
    implements JsonIndexerFactory {
  @Override
  public JsonIndexer createIndexer() {
    return new GeoJsonGenericAttributeIndexer();
  }
}
