package io.georocket.input.geojson

import de.undercouch.actson.JsonEvent
import io.georocket.input.Splitter
import io.georocket.input.json.JsonSplitter
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.JsonChunkMeta
import io.georocket.util.JsonStreamEvent
import io.georocket.util.StringWindow

/**
 * A JSON splitter that is tailored to GeoJSON files
 * @author Michel Kraemer
 */
class GeoJsonSplitter(window: StringWindow) : JsonSplitter(window) {
  /**
   * True if we're currently parsing a 'type' field
   */
  private var parsingType = false

  /**
   * The last 'type' parsed
   */
  private var lastType: String? = null

  /**
   * The level in which [.highestType] was found
   */
  private var highestTypeLevel = Int.MAX_VALUE

  /**
   * The value of the 'type' attribute found at the highest level of the
   * JSON file
   */
  private var highestType: String? = null

  override fun onEvent(event: JsonStreamEvent): Splitter.Result<JsonChunkMeta>? {
    val prevResultsCreated = resultsCreated
    var r = super.onEvent(event)
    when (event.event) {
      JsonEvent.FIELD_NAME -> if (mark == -1L || markedLevel == inArray.size + insideLevel - 1) {
        // we're inside a chunk. try to get its type
        parsingType = "type" == event.currentValue
      }
      JsonEvent.VALUE_STRING -> if (parsingType) {
        if (mark == -1L && inArray.size < highestTypeLevel) {
          // we're not inside a chunk. store the type from the highest level.
          highestTypeLevel = inArray.size
          highestType = event.currentValue.toString()
        } else {
          // we're inside a chunk
          lastType = event.currentValue.toString()
        }
      }
      JsonEvent.EOF -> {
        // We reached the end of the file and no chunk has been created yet. We
        // should set 'lastType' to the type that we found on the highest level
        // in the file so that the chunk that we are going to create will get
        // this type.
        if (!prevResultsCreated) {
          lastType = highestType
        }
        parsingType = false
      }
      else -> parsingType = false
    }
    val lt = lastType
    if (r != null && lt != null) {
      r = Splitter.Result(r.chunk, null, null, GeoJsonChunkMeta(lt, r.meta))
      lastType = null
    }
    return r
  }

  override fun makeResult(pos: Long): Splitter.Result<JsonChunkMeta>? {
    if (lastFieldName == null && ("FeatureCollection" == highestType || "GeometryCollection" == highestType)) {
      // ignore empty collections
      return null
    }

    // ignore all chunks found in extra attributes not part of the standard
    return if (lastFieldName == null || lastFieldName == "features" || lastFieldName == "geometries") {
      super.makeResult(pos)
    } else null
  }
}
