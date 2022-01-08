package io.georocket.input.geojson

import io.georocket.index.geojson.JsonTransformer
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.util.StringWindow
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

/**
 * Test for [GeoJsonSplitter]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class GeoJsonSplitterTest {
  private suspend fun split(file: String): List<Pair<GeoJsonChunkMeta, JsonObject>> {
    val json = javaClass.getResource(file)!!.readBytes()

    val buf = Buffer.buffer(json)
    val window = StringWindow()
    window.append(buf)

    val chunks = mutableListOf<Pair<GeoJsonChunkMeta, JsonObject>>()
    val splitter = GeoJsonSplitter(window)

    JsonTransformer().transform(body = buf).collect { e ->
      val r = splitter.onEvent(e)
      if (r != null) {
        val o = JsonObject(r.chunk)
        assertThat(r.prefix).isNull()
        assertThat(r.suffix).isNull()
        chunks.add(r.meta as GeoJsonChunkMeta to o)
      }
    }

    return chunks
  }

  /**
   * Test if a Feature can be split correctly
   */
  @Test
  fun feature(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "feature.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks.size.toLong()).isEqualTo(1)

        val t1 = chunks[0]
        val m1 = t1.first
        assertThat(m1.parentFieldName).isNull()
        assertThat(m1.type).isEqualTo("Feature")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("Feature")
        assertThat(o1.getJsonObject("properties").getString("name")).isEqualTo("Fraunhofer IGD")
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a Feature with a greek property can be split correctly
   */
  @Test
  fun greek(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "greek.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).hasSize(1)

        val t1 = chunks[0]
        val m1 = t1.first
        assertThat(m1.parentFieldName).isNull()
        assertThat(m1.type).isEqualTo("Feature")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("Feature")
        assertThat(o1.getJsonObject("properties").getString("name")).isEqualTo(
          "\u03a1\u039f\u0394\u0391\u039a\u0399\u039d\u0399\u0395\u03a3 " +
              "\u039c\u0395\u03a4\u0391\u03a0\u039f\u0399\u0397\u03a3\u0397\u03a3"
        )
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a FeatureCollection can be split correctly
   */
  @Test
  fun featureCollection(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "featurecollection.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).hasSize(2)

        val t1 = chunks[0]
        val m1 = t1.first
        assertThat(m1.parentFieldName).isEqualTo("features")
        assertThat(m1.type).isEqualTo("Feature")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("Feature")
        assertThat(o1.getJsonObject("properties").getString("name")).isEqualTo("Fraunhofer IGD")

        val t2 = chunks[1]
        val m2 = t2.first
        assertThat(m2.parentFieldName).isEqualTo("features")
        assertThat(m2.type).isEqualTo("Feature")

        val o2 = t2.second
        assertThat(o2.getString("type")).isEqualTo("Feature")
        assertThat(o2.getJsonObject("properties").getString("name")).isEqualTo("Darmstadtium")
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a GeometryCollection can be split correctly
   */
  @Test
  fun geometryCollection(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "geometrycollection.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).hasSize(2)

        val t1 = chunks[0]
        val m1 = t1.first
        assertThat(m1.parentFieldName).isEqualTo("geometries")
        assertThat(m1.type).isEqualTo("Point")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("Point")
        assertThat(o1.getJsonArray("coordinates").getDouble(0).toDouble()).isEqualTo(8.6599)

        val t2 = chunks[1]
        val m2 = t2.first
        assertThat(m2.parentFieldName).isEqualTo("geometries")
        assertThat(m2.type).isEqualTo("Point")

        val o2 = t2.second
        assertThat(o2.getString("type")).isEqualTo("Point")
        assertThat(o2.getJsonArray("coordinates").getDouble(0).toDouble()).isEqualTo(8.6576)
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a LineString can be split correctly
   */
  @Test
  fun lineString(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "linestring.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).hasSize(1)

        val t1 = chunks[0]
        val m1 = t1.first
        assertThat(m1.parentFieldName).isNull()
        assertThat(m1.type).isEqualTo("LineString")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("LineString")
        assertThat(o1.getJsonArray("coordinates").size().toLong()).isEqualTo(13)
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a MultiLineString can be split correctly
   */
  @Test
  fun muliLineString(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "multilinestring.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).hasSize(1)

        val t1 = chunks[0]
        val m1 = t1.first
        assertThat(m1.parentFieldName).isNull()
        assertThat(m1.type).isEqualTo("MultiLineString")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("MultiLineString")
        assertThat(o1.getJsonArray("coordinates")).hasSize(3)
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a MultiPoint can be split correctly
   */
  @Test
  fun multiPoint(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "multipoint.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).hasSize(1)

        val t1 = chunks[0]
        val m1 = t1.first
        assertThat(m1.parentFieldName).isNull()
        assertThat(m1.type).isEqualTo("MultiPoint")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("MultiPoint")
        assertThat(o1.getJsonArray("coordinates")).hasSize(2)
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a MultiPolygon can be split correctly
   */
  @Test
  fun multiPolygon(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "multipolygon.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).hasSize(1)

        val t1 = chunks[0]
        val m1 = t1.first
        assertThat(m1.parentFieldName).isNull()
        assertThat(m1.type).isEqualTo("MultiPolygon")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("MultiPolygon")
        assertThat(o1.getJsonArray("coordinates")).hasSize(1)
        assertThat(o1.getJsonArray("coordinates").getJsonArray(0)).hasSize(1)
        assertThat(o1.getJsonArray("coordinates").getJsonArray(0).getJsonArray(0)).hasSize(13)
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a Point can be split correctly
   */
  @Test
  fun point(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "point.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).hasSize(1)

        val t1 = chunks[0]
        val m1 = t1.first
        assertThat(m1.parentFieldName).isNull()
        assertThat(m1.type).isEqualTo("Point")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("Point")
        assertThat(o1.getJsonArray("coordinates")).hasSize(2)
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if a Polygon can be split correctly
   */
  @Test
  fun polygon(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "polygon.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).hasSize(1)

        val t1 = chunks[0]
        val m1 = t1.first

        assertThat(m1.parentFieldName).isNull()
        assertThat(m1.type).isEqualTo("Polygon")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("Polygon")
        assertThat(o1.getJsonArray("coordinates")).hasSize(1)
        assertThat(o1.getJsonArray("coordinates").getJsonArray(0)).hasSize(13)
      }

      ctx.completeNow()
    }
  }

  /**
   * Test if extra attributes not part of the standard are ignored
   */
  @Test
  fun extraAttributes(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "featurecollectionext.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).hasSize(2)

        val t1 = chunks[0]
        val m1 = t1.first
        assertThat(m1.parentFieldName).isEqualTo("features")
        assertThat(m1.type).isEqualTo("Feature")

        val o1 = t1.second
        assertThat(o1.getString("type")).isEqualTo("Feature")
        assertThat(o1.getJsonObject("properties").getString("name")).isEqualTo("Fraunhofer IGD")

        val t2 = chunks[1]
        val m2 = t2.first
        assertThat(m2.parentFieldName).isEqualTo("features")
        assertThat(m2.type).isEqualTo("Feature")

        val o2 = t2.second
        assertThat(o2.getString("type")).isEqualTo("Feature")
        assertThat(o2.getJsonObject("properties").getString("name")).isEqualTo("Darmstadtium")
      }

      ctx.completeNow()
    }
  }

  /**
   * Make sure the splitter doesn't find any chunks in an empty feature collection
   */
  @Test
  fun emptyFeatureCollection(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "featurecollectionempty.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).isEmpty()
      }

      ctx.completeNow()
    }
  }

  /**
   * Make sure the splitter doesn't find any chunks in an empty geometry collection
   */
  @Test
  fun emptyGeometryCollection(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val filename = "geometrycollectionempty.json"
      val chunks = split(filename)

      ctx.verify {
        assertThat(chunks).isEmpty()
      }

      ctx.completeNow()
    }
  }
}
