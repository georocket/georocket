package io.georocket.index.geojson

import de.undercouch.actson.JsonEvent
import io.georocket.index.generic.BoundingBoxIndexer
import io.georocket.index.xml.JsonIndexer
import io.georocket.util.JsonStreamEvent

/**
 * Indexes bounding boxes of GeoJSON chunks
 * @author Michel Kraemer
 */
class GeoJsonBoundingBoxIndexer : BoundingBoxIndexer(), JsonIndexer {
  /**
   * The current level in the parsed GeoJSON chunk. Will be increased
   * every time the start of an object or an array is found and decreased
   * every time the object or array is closed.
   */
  private var level = 0

  /**
   * The level where the `coordinates` field was found. The value
   * is `-1` if the field has not been found yet or if we already
   * scanned past it.
   */
  private var coordinatesLevel = -1

  /**
   * The last encountered X ordinate
   */
  private var currentX = 0.0

  /**
   * `0` if the next number found should be the X ordinate,
   * `1` if it should be Y.
   */
  private var currentOrdinate = 0

  override fun onEvent(event: JsonStreamEvent) {
    when (event.event) {
      JsonEvent.START_ARRAY, JsonEvent.START_OBJECT -> level++

      JsonEvent.FIELD_NAME -> if ("coordinates" == event.currentValue) {
        currentOrdinate = 0
        coordinatesLevel = level
      }

      JsonEvent.VALUE_INT, JsonEvent.VALUE_DOUBLE -> if (coordinatesLevel >= 0) {
        val d = (event.currentValue as Number).toDouble()
        if (currentOrdinate == 0) {
          currentX = d
          currentOrdinate = 1
        } else if (currentOrdinate == 1) {
          addToBoundingBox(currentX, d)
          currentOrdinate = 0
        }
      }

      JsonEvent.END_ARRAY, JsonEvent.END_OBJECT -> {
        level--
        if (coordinatesLevel == level) {
          coordinatesLevel = -1
        }
      }
    }
  }
}
