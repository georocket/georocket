package io.georocket.index.xml

import io.georocket.coVerify
import io.georocket.util.XMLStreamEvent
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import javax.xml.stream.events.XMLEvent

/**
 * Test [XMLTransformer]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class XMLTransformerTest {
  companion object {
    /**
     * Test input data
     */
    private const val XMLHEADER =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
    private const val XML_CHUNK1 = "<test>"
    private const val XML_CHUNK2 = "</test>"
    private const val XML = XMLHEADER + XML_CHUNK1 + XML_CHUNK2

    /**
     * The expected events
     */
    private val EXPECTED_EVENTS = listOf(
      // (position, event)
      0L to XMLEvent.START_DOCUMENT,
      0L to XMLEvent.START_ELEMENT,
      62L to XMLEvent.END_ELEMENT,
      69L to XMLEvent.END_DOCUMENT
    )
  }

  private val expectedEvents = ArrayDeque(EXPECTED_EVENTS)

  private fun onEvent(e: XMLStreamEvent) {
    val (expectedPos, expectedEvent) = expectedEvents.removeFirst()
    Assertions.assertThat(e.event).isEqualTo(expectedEvent)
    Assertions.assertThat(e.pos).isEqualTo(expectedPos)
  }

  private fun onEnd() {
    Assertions.assertThat(expectedEvents).isEmpty()
  }

  /**
   * Test if a simple XML string can be parsed
   */
  @Test
  fun parseSimple(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      ctx.coVerify {
        XMLTransformer().transform(Buffer.buffer(XMLHEADER),
          Buffer.buffer(XML_CHUNK1), Buffer.buffer(XML_CHUNK2)).collect { e ->
          onEvent(e)
        }
        onEnd()
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a simple XML string can be parsed when split into two chunks
   */
  @Test
  fun parseChunks(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      ctx.coVerify {
        val t = XMLTransformer()
        for (c in listOf(XMLHEADER, XML_CHUNK1, XML_CHUNK2)) {
          t.transformChunk(Buffer.buffer(c)).collect { onEvent(it) }
        }
        t.finish().collect { onEvent(it) }
        onEnd()
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a simple XML string can be parsed when split into arbitrary chunks
   */
  @Test
  fun parseIncompleteElements(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      ctx.coVerify {
        val t = XMLTransformer()
        for (c in listOf(XML.substring(0, XMLHEADER.length + 3),
            XML.substring(XMLHEADER.length + 3, 64), XML.substring(64))) {
          t.transformChunk(Buffer.buffer(c)).collect { onEvent(it) }
        }
        t.finish().collect { onEvent(it) }
        onEnd()
      }

      ctx.completeNow()
    }
  }
}
