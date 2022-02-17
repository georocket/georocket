package io.georocket.index.geojson

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

/**
 * Tests [GeoJsonIdIndexer]
 * @author Tobias Dorra
 */
@ExtendWith(VertxExtension::class)
class GeoJsonIdIndexerTest {

  /**
   * Test if a JSON file containing a Feature can be indexed
   */
  @Test
  fun feature(ctx: VertxTestContext, vertx: Vertx) {
    val input = javaClass.getResource("feature.json")!!.readText()
    val expected = mapOf(
      "geoJsonFeatureIds" to listOf("fid")
    )

    CoroutineScope(vertx.dispatcher()).launch {
      val indexer = GeoJsonIdIndexer()
      JsonTransformer().transform(body = Buffer.buffer(input)).collect { e ->
        indexer.onEvent(e)
      }
      val output = indexer.makeResult()
      ctx.verify {
        Assertions.assertThat(output).isEqualTo(expected)
      }
      ctx.completeNow()
    }

  }
}
