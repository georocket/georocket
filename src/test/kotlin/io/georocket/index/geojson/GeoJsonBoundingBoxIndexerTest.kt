package io.georocket.index.geojson

import io.georocket.coVerify
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
 * Tests [GeoJsonBoundingBoxIndexer]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class GeoJsonBoundingBoxIndexerTest {
  /**
   * Indexes the given JSON file and checks if the result matches the
   * expected bounding box
   */
  private suspend fun assertIndexed(expectedType: String,
      expectedCoordinates: List<Any>, jsonFile: String) {
    val json = javaClass.getResource(jsonFile)!!.readText()

    val indexer = GeoJsonBoundingBoxIndexer()
    val expectedMap = mapOf(
      "bbox" to mapOf(
        "type" to expectedType,
        "coordinates" to expectedCoordinates
      )
    )

    JsonTransformer().transform(null, Buffer.buffer(json), null).collect { e ->
      indexer.onEvent(e)
    }

    assertThat(indexer.makeResult()).isEqualTo(expectedMap)
  }

  /**
   * Test if a JSON file containing a Point geometry can be indexed
   */
  @Test
  fun point(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val expected = listOf(
        8.6599, 49.87424
      )

      ctx.coVerify {
        assertIndexed("Point", expected, "point.json")
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a JSON file containing a LineString geometry can be indexed
   */
  @Test
  fun lineString(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val expected = listOf(
        listOf(
          listOf(8.0, 49.0),
          listOf(8.5, 49.0),
          listOf(8.5, 49.5),
          listOf(8.0, 49.5),
          listOf(8.0, 49.0)
        )
      )

      ctx.coVerify {
        assertIndexed("Polygon", expected, "linestring.json")
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a JSON file containing a Feature with a LineString geometry
   * can be indexed
   */
  @Test
  fun feature(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val expected = listOf(
        listOf(
          listOf(8.0, 49.0),
          listOf(8.5, 49.0),
          listOf(8.5, 49.5),
          listOf(8.0, 49.5),
          listOf(8.0, 49.0)
        )
      )

      ctx.coVerify {
        assertIndexed("Polygon", expected, "feature.json")
      }

      ctx.completeNow()
    }
  }
}
