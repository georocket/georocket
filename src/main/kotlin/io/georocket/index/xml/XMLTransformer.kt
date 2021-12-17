package io.georocket.index.xml

import com.fasterxml.aalto.AsyncXMLStreamReader
import com.fasterxml.aalto.stax.InputFactoryImpl
import io.georocket.index.Transformer
import io.georocket.util.XMLStreamEvent
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow

/**
 * Parses XML chunks and transforms them to stream events
 * @author Michel Kraemer
 */
class XMLTransformer : Transformer<XMLStreamEvent> {
  private val parser = InputFactoryImpl().createAsyncForByteArray()

  private suspend fun processEvents() = flow {
    while (true) {
      val nextEvent = parser.next()
      if (nextEvent == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
        break
      }

      val pos = parser.location.characterOffset
      val streamEvent = XMLStreamEvent(nextEvent, pos, parser)
      emit(streamEvent)

      if (nextEvent == AsyncXMLStreamReader.END_DOCUMENT) {
        parser.close()
        break
      }
    }
  }

  override suspend fun transformChunk(chunk: Buffer) = flow {
    val bytes = chunk.bytes
    var i = 0
    while (i < bytes.size) {
      val len = bytes.size - i
      parser.inputFeeder.feedInput(bytes, i, len)
      i += len
      emitAll(processEvents())
    }
  }

  override suspend fun finish() = flow {
    parser.inputFeeder.endOfInput()
    emitAll(processEvents())
  }
}
