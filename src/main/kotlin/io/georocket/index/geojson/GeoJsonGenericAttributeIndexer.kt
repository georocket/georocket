package io.georocket.index.geojson

import de.undercouch.actson.JsonEvent.END_ARRAY
import de.undercouch.actson.JsonEvent.END_OBJECT
import de.undercouch.actson.JsonEvent.FIELD_NAME
import de.undercouch.actson.JsonEvent.START_ARRAY
import de.undercouch.actson.JsonEvent.START_OBJECT
import de.undercouch.actson.JsonEvent.VALUE_DOUBLE
import de.undercouch.actson.JsonEvent.VALUE_FALSE
import de.undercouch.actson.JsonEvent.VALUE_INT
import de.undercouch.actson.JsonEvent.VALUE_NULL
import de.undercouch.actson.JsonEvent.VALUE_STRING
import de.undercouch.actson.JsonEvent.VALUE_TRUE
import io.georocket.index.generic.GenericAttributeIndexer
import io.georocket.util.JsonStreamEvent

/**
 * Indexer for properties in GeoJSON features
 * @author Michel Kraemer
 */
class GeoJsonGenericAttributeIndexer : GenericAttributeIndexer<JsonStreamEvent>() {
  /**
   * The current level in the parsed GeoJSON chunk. Will be increased
   * every time the start of an object or an array is found and decreased
   * every time the object or array is closed.
   */
  private var level = 0

  /**
   * The level where the `properties` field was found. The value
   * is `-1` if the field has not been found yet or if we already
   * scanned past it.
   */
  private var propertiesLevel = -1

  /**
   * The last key found in the properties field
   */
  private var currentKey: String? = null

  override fun onEvent(event: JsonStreamEvent) {
    when (event.event) {
      START_ARRAY, START_OBJECT -> level++

      FIELD_NAME -> if (propertiesLevel >= 0) {
        if (propertiesLevel == level - 1) {
          currentKey = event.currentValue.toString()
        }
      } else if ("properties" == event.currentValue) {
        currentKey = null
        propertiesLevel = level
      }

      VALUE_STRING, VALUE_INT, VALUE_DOUBLE, VALUE_TRUE, VALUE_FALSE, VALUE_NULL -> {
        val value = eventValueToString(event)
        if (currentKey != null && propertiesLevel == level - 1) {
          put(currentKey!!, value)
        }
      }

      END_ARRAY, END_OBJECT -> {
        level--
        if (propertiesLevel == level) {
          propertiesLevel = -1
        }
      }
    }
  }

  /**
   * Gets the value from the given JSON event and converts it to a String
   */
  private fun eventValueToString(event: JsonStreamEvent): String {
    return when (event.event) {
      VALUE_STRING, VALUE_INT, VALUE_DOUBLE -> event.currentValue.toString()
      VALUE_TRUE -> "true"
      VALUE_FALSE -> "false"
      VALUE_NULL -> "null"
      else -> throw IllegalArgumentException("Event type must be a value type")
    }
  }
}
