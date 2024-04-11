package io.georocket.index.geojson

import de.undercouch.actson.JsonEvent
import de.undercouch.actson.JsonParser
import io.georocket.index.Transformer
import io.georocket.util.JsonStreamEvent
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow

/**
 * Parses JSON chunks and transforms them to stream events
 * @author Michel Kraemer
 */
class JsonTransformer : Transformer<JsonStreamEvent> {
  private val parser = JsonParser()

  private suspend fun processEvents() = flow {
    while (true) {
      val nextEvent = parser.nextEvent()
      val value = when (nextEvent) {
        JsonEvent.NEED_MORE_INPUT -> break
        JsonEvent.ERROR -> throw IllegalStateException("Syntax error")
        JsonEvent.VALUE_STRING, JsonEvent.FIELD_NAME -> parser.currentString
        JsonEvent.VALUE_DOUBLE -> parser.currentDouble
        JsonEvent.VALUE_INT -> parser.currentInt
        else -> null
      }

      val streamEvent = JsonStreamEvent(nextEvent, parser.parsedCharacterCount, value)
      emit(streamEvent)

      if (nextEvent == JsonEvent.EOF) {
        break
      }
    }
  }

  override suspend fun transformChunk(chunk: Buffer) = flow {
    val bytes = chunk.bytes
    var i = 0
    while (i < bytes.size) {
      i += parser.feeder.feed(bytes, i, bytes.size - i)
      emitAll(processEvents())
    }
  }

  override suspend fun finish() = flow {
    parser.feeder.done()
    emitAll(processEvents())
  }
}
