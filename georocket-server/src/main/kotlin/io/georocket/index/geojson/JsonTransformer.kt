package io.georocket.index.geojson

import de.undercouch.actson.JsonEvent
import de.undercouch.actson.JsonParser
import io.georocket.index.Transformer
import io.georocket.util.JsonStreamEvent
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.channels.Channel

/**
 * Parses JSON chunks and transforms them to stream events
 * @author Michel Kraemer
 */
class JsonTransformer : Transformer<JsonStreamEvent> {
  override suspend fun transformTo(chunk: Buffer, destination: Channel<JsonStreamEvent>) {
    val parser = JsonParser()

    val processEvents: suspend () -> Boolean = pe@{
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
        destination.send(streamEvent)

        if (nextEvent == JsonEvent.EOF) {
          return@pe false
        }
      }
      true
    }

    val bytes = chunk.bytes
    var i = 0
    while (i < bytes.size) {
      i += parser.feeder.feed(bytes, i, bytes.size - i)
      if (!processEvents()) {
        break
      }
    }

    // process remaining events
    parser.feeder.done()
    processEvents()

    destination.close()
  }
}
