package io.georocket.index.geojson;

import io.georocket.index.generic.BoundingBoxIndexerFactory;
import io.georocket.index.xml.JsonIndexer;
import io.georocket.index.xml.JsonIndexerFactory;

/**
 * Create instances of {@link GeoJsonBoundingBoxIndexer}
 * @author Michel Kraemer
 */
public class GeoJsonBoundingBoxIndexerFactory extends BoundingBoxIndexerFactory
    implements JsonIndexerFactory {
  @Override
  public JsonIndexer createIndexer() {
    return new GeoJsonBoundingBoxIndexer();
  }
}
