package io.georocket.output.geojson

import io.georocket.assertThatThrownBy
import io.georocket.coVerify
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.util.io.BufferWriteStream
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

/**
 * Test [GeoJsonMerger]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class GeoJsonMergerTest {
  private fun doMerge(vertx: Vertx, ctx: VertxTestContext, chunks: List<Buffer>,
      metas: List<GeoJsonChunkMeta>, jsonContents: String, optimistic: Boolean = false) {
    val m = GeoJsonMerger(optimistic)
    val bws = BufferWriteStream()

    if (!optimistic) {
      for (meta in metas) {
        m.init(meta)
      }
    }

    GlobalScope.launch(vertx.dispatcher()) {
      ctx.coVerify {
        for ((meta, chunk) in metas.zip(chunks)) {
          val stream = DelegateChunkReadStream(chunk)
          m.merge(stream, meta, bws)
        }
        m.finish(bws)
        assertThat(bws.buffer.toString("utf-8")).isEqualTo(jsonContents)
      }
      ctx.completeNow()
    }
  }

  /**
   * Test if one geometry is rendered directly
   */
  @Test
  fun oneGeometry(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Polygon"}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
    doMerge(vertx, ctx, listOf(chunk1), listOf(cm1), strChunk1)
  }

  /**
   * Test if one geometry can be merged in optimistic mode
   */
  @Test
  fun oneGeometryOptimistic(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Polygon"}"""
    val expected = """{"type":"FeatureCollection","features":[""" +
      """{"type":"Feature","geometry":$strChunk1}]}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
    doMerge(vertx, ctx, listOf(chunk1), listOf(cm1), expected, true)
  }

  /**
   * Test if one feature is rendered directly
   */
  @Test
  fun oneFeature(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    doMerge(vertx, ctx, listOf(chunk1), listOf(cm1), strChunk1)
  }

  /**
   * Test if one feature can be merged in optimistic mode
   */
  @Test
  fun oneFeatureOptimistic(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val expected = """{"type":"FeatureCollection","features":[$strChunk1]}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    doMerge(vertx, ctx, listOf(chunk1), listOf(cm1), expected, true)
  }

  /**
   * Test if two geometries can be merged to a geometry collection
   */
  @Test
  fun twoGeometries(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Polygon"}"""
    val strChunk2 = """{"type":"Point"}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length())
    doMerge(vertx, ctx, listOf(chunk1, chunk2), listOf(cm1, cm2),
        "{\"type\":\"GeometryCollection\",\"geometries\":[$strChunk1,$strChunk2]}")
  }

  /**
   * Test if two geometries can be merged in optimistic mode
   */
  @Test
  fun twoGeometriesOptimistic(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Polygon"}"""
    val strChunk2 = """{"type":"Point"}"""
    val expected = """{"type":"FeatureCollection","features":[""" +
        """{"type":"Feature","geometry":$strChunk1},""" +
        """{"type":"Feature","geometry":$strChunk2}]}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length())
    doMerge(vertx, ctx, listOf(chunk1, chunk2), listOf(cm1, cm2), expected, true)
  }

  /**
   * Test if three geometries can be merged to a geometry collection
   */
  @Test
  fun threeGeometries(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Polygon"}"""
    val strChunk2 = """{"type":"Point"}"""
    val strChunk3 = """{"type":"MultiPoint"}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val chunk3 = Buffer.buffer(strChunk3)
    val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length())
    val cm3 = GeoJsonChunkMeta("MultiPoint", "geometries", 0, chunk3.length())
    doMerge(vertx, ctx, listOf(chunk1, chunk2, chunk3), listOf(cm1, cm2, cm3),
        """{"type":"GeometryCollection","geometries":[$strChunk1,$strChunk2,$strChunk3]}""")
  }

  /**
   * Test if two features can be merged to a feature collection
   */
  @Test
  fun twoFeatures(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val strChunk2 = """{"type":"Feature","properties":{}}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
    doMerge(vertx, ctx, listOf(chunk1, chunk2), listOf(cm1, cm2),
        """{"type":"FeatureCollection","features":[$strChunk1,$strChunk2]}""")
  }

  /**
   * Test if two features can be merged in optimistic mode
   */
  @Test
  fun twoFeaturesOptimistic(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val strChunk2 = """{"type":"Feature","properties":{}}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
    doMerge(vertx, ctx, listOf(chunk1, chunk2), listOf(cm1, cm2),
        """{"type":"FeatureCollection","features":[$strChunk1,$strChunk2]}""", true)
  }

  /**
   * Test if three features can be merged to a feature collection
   */
  @Test
  fun threeFeatures(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val strChunk2 = """{"type":"Feature","properties":{}}"""
    val strChunk3 = """{"type":"Feature","geometry":[]}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val chunk3 = Buffer.buffer(strChunk3)
    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
    val cm3 = GeoJsonChunkMeta("Feature", "features", 0, chunk3.length())
    doMerge(vertx, ctx, listOf(chunk1, chunk2, chunk3), listOf(cm1, cm2, cm3),
        """{"type":"FeatureCollection","features":[$strChunk1,$strChunk2,$strChunk3]}""")
  }

  /**
   * Test if two geometries and a feature can be merged to a feature collection
   */
  @Test
  fun geometryAndFeature(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Polygon"}"""
    val strChunk2 = """{"type":"Feature"}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
    doMerge(vertx, ctx, listOf(chunk1, chunk2), listOf(cm1, cm2),
        """{"type":"FeatureCollection","features":[""" +
            """{"type":"Feature","geometry":$strChunk1},$strChunk2]}""")
  }

  /**
   * Test if two geometries and a feature can be merged to a feature collection
   */
  @Test
  fun featureAndGeometry(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val strChunk2 = """{"type":"Polygon"}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk2.length())
    doMerge(vertx, ctx, listOf(chunk1, chunk2), listOf(cm1, cm2),
        """{"type":"FeatureCollection","features":[$strChunk1,""" +
            """{"type":"Feature","geometry":$strChunk2}]}""")
  }

  /**
   * Test if two geometries and a feature can be merged to a feature collection
   */
  @Test
  fun twoGeometriesAndAFeature(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Polygon"}"""
    val strChunk2 = """{"type":"Point"}"""
    val strChunk3 = """{"type":"Feature"}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val chunk3 = Buffer.buffer(strChunk3)
    val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length())
    val cm3 = GeoJsonChunkMeta("Feature", "features", 0, chunk3.length())
    doMerge(vertx, ctx, listOf(chunk1, chunk2, chunk3), listOf(cm1, cm2, cm3),
        """{"type":"FeatureCollection","features":[""" +
            """{"type":"Feature","geometry":$strChunk1},""" +
            """{"type":"Feature","geometry":$strChunk2},$strChunk3]}""")
  }

  /**
   * Test if two geometries and a feature can be merged to a feature collection
   */
  @Test
  fun twoFeaturesAndAGeometry(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val strChunk2 = """{"type":"Feature","properties":{}}"""
    val strChunk3 = """{"type":"Point"}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val chunk3 = Buffer.buffer(strChunk3)
    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
    val cm3 = GeoJsonChunkMeta("Point", "geometries", 0, chunk3.length())
    doMerge(vertx, ctx, listOf(chunk1, chunk2, chunk3), listOf(cm1, cm2, cm3),
        """{"type":"FeatureCollection","features":[$strChunk1,$strChunk2,""" +
            """{"type":"Feature","geometry":$strChunk3}]}""")
  }

  /**
   * Test if the merger fails if [GeoJsonMerger.init] has
   * not been called often enough
   */
  @Test
  fun notEnoughInits(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val strChunk2 = """{"type":"Feature","properties":{}}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
    val m = GeoJsonMerger(false)
    val bws = BufferWriteStream()

    GlobalScope.launch(vertx.dispatcher()) {
      ctx.coVerify {
        assertThatThrownBy {
          m.init(cm1)
          m.merge(DelegateChunkReadStream(chunk1), cm1, bws)
          m.merge(DelegateChunkReadStream(chunk2), cm2, bws)
        }.isInstanceOf(IllegalStateException::class.java)
      }
      ctx.completeNow()
    }
  }

  /**
   * Test if the merger succeeds if [GeoJsonMerger.init] has
   * not been called just often enough
   */
  @Test
  fun enoughInits(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val strChunk2 = """{"type":"Feature","properties":{}}"""
    val strChunk3 = """{"type":"Polygon"}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)
    val chunk3 = Buffer.buffer(strChunk3)
    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
    val cm3 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk2.length())
    val jsonContents = """{"type":"FeatureCollection","features":""" +
        """[$strChunk1,$strChunk2,{"type":"Feature","geometry":$strChunk3}]}"""
    val m = GeoJsonMerger(false)
    val bws = BufferWriteStream()

    GlobalScope.launch(vertx.dispatcher()) {
      ctx.coVerify {
        m.init(cm1)
        m.init(cm2)
        m.merge(DelegateChunkReadStream(chunk1), cm1, bws)
        m.merge(DelegateChunkReadStream(chunk2), cm2, bws)
        m.merge(DelegateChunkReadStream(chunk3), cm3, bws)
        m.finish(bws)

        assertThat(jsonContents).isEqualTo(bws.buffer.toString("utf-8"))
      }
      ctx.completeNow()
    }
  }
}