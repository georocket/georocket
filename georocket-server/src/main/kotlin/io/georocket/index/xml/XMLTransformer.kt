package io.georocket.index.xml

import com.fasterxml.aalto.AsyncXMLStreamReader
import com.fasterxml.aalto.stax.InputFactoryImpl
import io.georocket.index.Transformer
import io.georocket.util.XMLStreamEvent
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.channels.Channel

/**
 * Parses XML chunks and transforms them to stream events
 * @author Michel Kraemer
 */
class XMLTransformer : Transformer<XMLStreamEvent> {
  override suspend fun transformTo(chunk: Buffer, destination: Channel<XMLStreamEvent>) {
    val parser = InputFactoryImpl().createAsyncForByteArray()

    val processEvents: suspend () -> Boolean = pe@{
      while (true) {
        val nextEvent = parser.next()
        if (nextEvent == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
          break
        }

        val pos = parser.location.characterOffset
        val streamEvent = XMLStreamEvent(nextEvent, pos, parser)
        destination.send(streamEvent)

        if (nextEvent == AsyncXMLStreamReader.END_DOCUMENT) {
          parser.close()
          return@pe false
        }
      }
      true
    }

    val bytes = chunk.bytes
    var i = 0
    while (i < bytes.size) {
      val len = bytes.size - i
      parser.inputFeeder.feedInput(bytes, i, len)
      i += len
      if (!processEvents()) {
        break
      }
    }

    // process remaining events
    parser.inputFeeder.endOfInput()
    processEvents()
  }
}
