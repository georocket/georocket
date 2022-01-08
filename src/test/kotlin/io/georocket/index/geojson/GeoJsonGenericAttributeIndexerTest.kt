package io.georocket.index.geojson

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

/**
 * Tests [GeoJsonGenericAttributeIndexer]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class GeoJsonGenericAttributeIndexerTest {
  /**
   * Indexes the given JSON file and checks if the result matches the
   * expected properties map
   */
  private fun assertIndexed(expected: List<Map<String, Any>>, jsonFile: String,
      ctx: VertxTestContext, vertx: Vertx) {
    val json = javaClass.getResource(jsonFile)!!.readText()

    val indexer = GeoJsonGenericAttributeIndexer()
    val expectedMap = mapOf("genAttrs" to expected)

    CoroutineScope(vertx.dispatcher()).launch {
      JsonTransformer().transform(body = Buffer.buffer(json)).collect { e ->
        indexer.onEvent(e)
      }

      ctx.verify {
        assertThat(indexer.makeResult()).isEqualTo(expectedMap)
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a JSON file containing a Feature can be indexed
   */
  @Test
  fun feature(ctx: VertxTestContext, vertx: Vertx) {
    val expected = listOf(
      mapOf("key" to "name", "value" to "My Feature"),
      mapOf("key" to "location", "value" to "Moon"),
      mapOf("key" to "owner", "value" to "Elvis"),
      mapOf("key" to "year", "value" to 2016L),
      mapOf("key" to "solid", "value" to "true")
    )
    assertIndexed(expected, "feature.json", ctx, vertx)
  }
}
