package io.georocket.index.geojson

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.collect
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
  private fun assertIndexed(expected: Map<String, Any>, jsonFile: String,
      ctx: VertxTestContext, vertx: Vertx) {
    val json = javaClass.getResource(jsonFile)!!.readText()

    val indexer = GeoJsonGenericAttributeIndexer()
    val expectedMap = mapOf("genAttrs" to expected)

    CoroutineScope(vertx.dispatcher()).launch {
      JsonTransformer().transform(Buffer.buffer(json)).collect { e ->
        indexer.onEvent(e)
      }

      ctx.verify {
        assertThat(indexer.result).isEqualTo(expectedMap)
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a JSON file containing a Feature can be indexed
   */
  @Test
  fun feature(ctx: VertxTestContext, vertx: Vertx) {
    val expected = mapOf(
      "name" to "My Feature",
      "location" to "Moon",
      "owner" to "Elvis",
      "year" to "2016",
      "solid" to "true"
    )
    assertIndexed(expected, "feature.json", ctx, vertx)
  }
}
