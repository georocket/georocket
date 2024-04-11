package io.georocket.index.geojson

import de.undercouch.actson.JsonEvent
import io.georocket.coVerify
import io.georocket.util.JsonStreamEvent
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

/**
 * Test [JsonTransformer]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class JsonTransformerTest {
  companion object {
    /**
     * Test input data
     */
    private const val JSON_CHUNK1 = "{\"name\":\""
    private const val JSON_CHUNK2 = "Elvis\"}"
    private const val JSON = JSON_CHUNK1 + JSON_CHUNK2

    /**
     * The expected events
     */
    private val EXPECTED_EVENTS = listOf(
      JsonStreamEvent(JsonEvent.START_OBJECT, 1),
      JsonStreamEvent(JsonEvent.FIELD_NAME, 7),
      JsonStreamEvent(JsonEvent.VALUE_STRING, 15),
      JsonStreamEvent(JsonEvent.END_OBJECT, 16),
      JsonStreamEvent(JsonEvent.EOF, 16)
    )
  }

  private lateinit var expectedEvents: ArrayDeque<JsonStreamEvent>

  /**
   * Create objects required for all tests
   */
  @BeforeEach
  fun setUp() {
    expectedEvents = ArrayDeque(EXPECTED_EVENTS)
  }

  private fun onEvent(e: JsonStreamEvent) {
    val expected = expectedEvents.removeFirst()
    assertThat(e.event).isEqualTo(expected.event)
    assertThat(e.pos).isEqualTo(expected.pos)
  }

  private fun onEnd() {
    assertThat(expectedEvents).isEmpty()
  }

  /**
   * Test if a simple JSON string can be parsed
   */
  @Test
  fun parseSimple(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      ctx.coVerify {
        JsonTransformer().transform(body = Buffer.buffer(JSON)).collect { e ->
          onEvent(e)
        }
        onEnd()
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a simple JSON string can be parsed when split into two chunks
   */
  @Test
  fun parseChunks(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      ctx.coVerify {
        val t = JsonTransformer()
        for (c in listOf(JSON_CHUNK1, JSON_CHUNK2)) {
          t.transformChunk(Buffer.buffer(c)).collect { onEvent(it) }
        }
        t.finish().collect { onEvent(it) }
        onEnd()
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a simple JSON string can be parsed when split into arbitrary chunks
   */
  @Test
  fun parseIncompleteElements(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      ctx.coVerify {
        val t = JsonTransformer()
        for (c in listOf(JSON.substring(0, 3), JSON.substring(3, 12), JSON.substring(12))) {
          t.transformChunk(Buffer.buffer(c)).collect { onEvent(it) }
        }
        t.finish().collect { onEvent(it) }
        onEnd()
      }

      ctx.completeNow()
    }
  }
}
