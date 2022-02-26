package io.georocket.index.geojson

import de.undercouch.actson.JsonEvent
import io.georocket.index.Indexer
import io.georocket.util.JsonStreamEvent
import java.util.Stack

/**
 * @author Tobias Dorra
 */
class GeoJsonIdIndexer: Indexer<JsonStreamEvent> {

  private data class ObjParseState(
    var currentKey: String? = null,
    var type: String? = null,
    var id: Any? = null,
  )

  private var ids = mutableListOf<Any>()
  private var state = Stack<ObjParseState>()

  override fun onEvent(event: JsonStreamEvent) {
    when (event.event) {
      JsonEvent.START_OBJECT -> {
        state.push(ObjParseState())
      }
      JsonEvent.END_OBJECT -> {
        if (!state.empty()) {
          val s = state.pop()
          val id = s.id
          if (s.type == "Feature" && id != null) {
            ids.add(id)
          }
        }
      }
      JsonEvent.FIELD_NAME -> {
        if (!state.empty()) {
          val s = state.peek()
          s.currentKey = event.currentValue.toString()
        }
      }
      JsonEvent.VALUE_STRING, JsonEvent.VALUE_INT -> {
        if (!state.empty()) {
          val s = state.peek()
          if (s.currentKey == "type") {
            s.type = event.currentValue.toString()
          }
          if (s.currentKey == "id") {
            s.id = event.currentValue
          }
        }
      }
    }
  }

  override fun makeResult(): Map<String, Any> {
    return if (ids.isNotEmpty()) {
      mapOf("geoJsonFeatureIds" to ids)
    } else {
      emptyMap()
    }
  }
}
