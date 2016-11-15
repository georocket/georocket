package io.georocket.index.geojson;

import de.undercouch.actson.JsonEvent;
import io.georocket.index.generic.BoundingBoxIndexer;
import io.georocket.index.xml.JsonIndexer;
import io.georocket.util.JsonStreamEvent;

/**
 * Indexes bounding boxes of GeoJSON chunks
 * @author Michel Kraemer
 */
public class GeoJsonBoundingBoxIndexer extends BoundingBoxIndexer
    implements JsonIndexer {
  /**
   * The current level in the parsed GeoJSON chunk. Will be increased
   * every time the start of an object or an array is found and decreased
   * every time the object or array is closed.
   */
  private int level = 0;
  
  /**
   * The level where the <code>coordinates</code> field was found. The value
   * is <code>-1</code> if the field has not been found yet or if we already
   * scanned past it.
   */
  private int coordinatesLevel = -1;
  
  /**
   * The last encountered X ordinate
   */
  private double currentX;
  
  /**
   * <code>0</code> if the next number found should be the X ordinate,
   * <code>1</code> if it should be Y.
   */
  private int currentOrdinate = 0;
  
  @Override
  public void onEvent(JsonStreamEvent event) {
    switch (event.getEvent()) {
    case JsonEvent.START_ARRAY:
    case JsonEvent.START_OBJECT:
      level++;
      break;
    
    case JsonEvent.FIELD_NAME:
      if ("coordinates".equals(event.getCurrentValue())) {
        currentOrdinate = 0;
        coordinatesLevel = level;
      }
      break;
    
    case JsonEvent.VALUE_INT:
    case JsonEvent.VALUE_DOUBLE:
      if (coordinatesLevel >= 0) {
        double d = ((Number)event.getCurrentValue()).doubleValue();
        if (currentOrdinate == 0) {
          currentX = d;
          currentOrdinate = 1;
        } else if (currentOrdinate == 1) {
          addToBoundingBox(currentX, d);
          currentOrdinate = 0;
        }
      }
      break;
    
    case JsonEvent.END_ARRAY:
    case JsonEvent.END_OBJECT:
      level--;
      if (coordinatesLevel == level) {
        coordinatesLevel = -1;
      }
      break;
    
    default:
      break;
    }
  }
}
