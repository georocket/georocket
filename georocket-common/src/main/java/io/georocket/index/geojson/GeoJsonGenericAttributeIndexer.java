package io.georocket.index.geojson;

import de.undercouch.actson.JsonEvent;
import io.georocket.index.generic.GenericAttributeIndexer;
import io.georocket.index.xml.JsonIndexer;
import io.georocket.util.JsonStreamEvent;

/**
 * Indexer for properties in GeoJSON features
 * @author Michel Kraemer
 */
public class GeoJsonGenericAttributeIndexer extends GenericAttributeIndexer
    implements JsonIndexer {
  /**
   * The current level in the parsed GeoJSON chunk. Will be increased
   * every time the start of an object or an array is found and decreased
   * every time the object or array is closed.
   */
  private int level = 0;
  
  /**
   * The level where the <code>properties</code> field was found. The value
   * is <code>-1</code> if the field has not been found yet or if we already
   * scanned past it.
   */
  private int propertiesLevel = -1;
  
  /**
   * The last key found in the properties field
   */
  private String currentKey;
  
  @Override
  public void onEvent(JsonStreamEvent event) {
    switch (event.getEvent()) {
    case JsonEvent.START_ARRAY:
    case JsonEvent.START_OBJECT:
      level++;
      break;
    
    case JsonEvent.FIELD_NAME:
      if (propertiesLevel >= 0) {
        if (propertiesLevel == level - 1) {
          currentKey = event.getCurrentValue().toString();
        }
      } else if ("properties".equals(event.getCurrentValue())) {
        currentKey = null;
        propertiesLevel = level;
      }
      break;
    
    case JsonEvent.VALUE_STRING:
    case JsonEvent.VALUE_INT:
    case JsonEvent.VALUE_DOUBLE:
    case JsonEvent.VALUE_TRUE:
    case JsonEvent.VALUE_FALSE:
    case JsonEvent.VALUE_NULL:
      String value = eventValueToString(event);
      if (currentKey != null && propertiesLevel == level - 1) {
        put(currentKey, value);
      }
      break;
    
    case JsonEvent.END_ARRAY:
    case JsonEvent.END_OBJECT:
      level--;
      if (propertiesLevel == level) {
        propertiesLevel = -1;
      }
      break;
    
    default:
      break;
    }
  }
  
  /**
   * Gets the value from the given JSON event and converts it to a String
   * @param event the event
   * @return the string representation of the event's value
   * @throws IllegalArgumentException if the JSON event does not refer to a value
   */
  private String eventValueToString(JsonStreamEvent event) {
    switch (event.getEvent()) {
    case JsonEvent.VALUE_STRING:
    case JsonEvent.VALUE_INT:
    case JsonEvent.VALUE_DOUBLE:
      return String.valueOf(event.getCurrentValue());
    
    case JsonEvent.VALUE_TRUE:
      return "true";
    
    case JsonEvent.VALUE_FALSE:
      return "false";
    
    case JsonEvent.VALUE_NULL:
      return "null";
    
    default:
      throw new IllegalArgumentException("Event type must be a value type");
    }
  }
}
