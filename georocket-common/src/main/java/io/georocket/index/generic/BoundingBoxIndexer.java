package io.georocket.index.generic;

import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import io.georocket.index.Indexer;
import io.georocket.index.xml.XMLBoundingBoxIndexer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Base class for all indexers that create Bounding Boxes
 * @author Michel Kraemer
 */
public class BoundingBoxIndexer implements Indexer {
  private static Logger log = LoggerFactory.getLogger(XMLBoundingBoxIndexer.class);

  /**
   * True if {@link #addToBoundingBox(double, double)} has been called
   * at least once
   */
  protected boolean boundingBoxInitialized = false;

  /**
   * The calculated bounding box of this chunk. Only contains valid values
   * if {@link #boundingBoxInitialized} is true
   */
  protected double minX;
  protected double maxX;
  protected double minY;
  protected double maxY;
  
  /**
   * Adds the given coordinate to the bounding box
   * @param x the x ordinate
   * @param y the y ordinate
   */
  protected void addToBoundingBox(double x, double y) {
    if (!boundingBoxInitialized) {
      minX = maxX = x;
      minY = maxY = y;
      boundingBoxInitialized = true;
    } else {
      if (x < minX) {
        minX = x;
      }
      if (x > maxX) {
        maxX = x;
      }
      if (y < minY) {
        minY = y;
      }
      if (y > maxY) {
        maxY = y;
      }
    }
  }
  
  /**
   * Check if the current bounding box contains valid values for WGS84
   * @return true if the current bounding box is valid, false otherwise
   */
  private boolean validate() {
    return (minX >= -180.0 && minY >= -90.0 && maxX <= 180.0 && maxY <= 90.0);
  }
  
  @Override
  public Map<String, Object> getResult() {
    if (!boundingBoxInitialized) {
      // the chunk's bounding box is unknown. do not add it to the index
      return ImmutableMap.of();
    }
    if (!validate()) {
      boundingBoxInitialized = false;
      log.warn("Invalid bounding box [" + minX + "," + minY + "," + maxX + "," + maxY + "]. "
          + "Values outside [-180.0, -90.0, 180.0, 90.0]. Skipping chunk.");
      return ImmutableMap.of();
    }
    //System.out.println(minX + "," + minY + "," + maxX + "," + maxY);
    return ImmutableMap.of("bbox", ImmutableMap.of(
        "type", "envelope",
        "coordinates", Arrays.asList(
            Arrays.asList(minX, maxY), // upper left
            Arrays.asList(maxX, minY)  // lower right
        )
    ));
  }
}
