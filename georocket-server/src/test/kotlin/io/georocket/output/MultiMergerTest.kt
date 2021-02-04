package io.georocket.output

import io.georocket.assertThatThrownBy
import io.georocket.coVerify
import io.georocket.storage.ChunkMeta
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.XMLStartElement
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
 * Tests for [MultiMerger]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class MultiMergerTest {
  companion object {
    private const val XMLHEADER = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>${"\n"}"""
  }

  private fun doMerge(vertx: Vertx, ctx: VertxTestContext, chunks: List<Buffer>,
      metas: List<ChunkMeta>, contents: String) {
    val m = MultiMerger(false)
    val bws = BufferWriteStream()

    for (meta in metas) {
      m.init(meta)
    }

    GlobalScope.launch(vertx.dispatcher()) {
      ctx.coVerify {
        for ((meta, chunk) in metas.zip(chunks)) {
          val stream = DelegateChunkReadStream(chunk)
          m.merge(stream, meta, bws)
        }
        m.finish(bws)
        assertThat(contents).isEqualTo(bws.buffer.toString("utf-8"))
      }
      ctx.completeNow()
    }
  }

  /**
   * Test if one GeoJSON geometry is rendered directly
   */
  @Test
  fun geoJsonOneGeometry(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Polygon"}"""
    val chunk1 = Buffer.buffer(strChunk1)
    val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())

    doMerge(vertx, ctx, listOf(chunk1), listOf(cm1), strChunk1)
  }

  /**
   * Test if two GeoJSON features can be merged to a feature collection
   */
  @Test
  fun geoJsonTwoFeatures(vertx: Vertx, ctx: VertxTestContext) {
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
   * Test if simple XML chunks can be merged
   */
  @Test
  fun xmlSimple(vertx: Vertx, ctx: VertxTestContext) {
    val chunk1 = Buffer.buffer("""$XMLHEADER<root><test chunk="1"></test></root>""")
    val chunk2 = Buffer.buffer("""$XMLHEADER<root><test chunk="2"></test></root>""")

    val cm = XMLChunkMeta(listOf(XMLStartElement("root")),
        XMLHEADER.length + 6, chunk1.length() - 7)

    doMerge(vertx, ctx, listOf(chunk1, chunk2), listOf(cm, cm),
        """$XMLHEADER<root><test chunk="1"></test><test chunk="2"></test></root>""")
  }

  /**
   * Test if the merger fails if chunks with a different type should be merged
   */
  @Test
  fun mixedInit(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val strChunk2 = """$XMLHEADER<root><test chunk="2"></test></root>"""

    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)

    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    val cm2 = XMLChunkMeta(listOf(XMLStartElement("root")),
        XMLHEADER.length + 6, chunk2.length() - 7)

    val m = MultiMerger(false)

    GlobalScope.launch(vertx.dispatcher()) {
      ctx.coVerify {
        m.init(cm1)
        assertThatThrownBy {
          m.init(cm2)
        }.isInstanceOf(IllegalStateException::class.java)
      }
      ctx.completeNow()
    }
  }

  /**
   * Test if the merger fails if chunks with a different type should be merged
   */
  @Test
  fun mixedMerge(vertx: Vertx, ctx: VertxTestContext) {
    val strChunk1 = """{"type":"Feature"}"""
    val strChunk2 = """$XMLHEADER<root><test chunk="2"></test></root>"""

    val chunk1 = Buffer.buffer(strChunk1)
    val chunk2 = Buffer.buffer(strChunk2)

    val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
    val cm2 = XMLChunkMeta(listOf(XMLStartElement("root")),
        XMLHEADER.length + 6, chunk2.length() - 7)

    val m = MultiMerger(false)
    val bws = BufferWriteStream()

    GlobalScope.launch(vertx.dispatcher()) {
      ctx.coVerify {
        m.init(cm1)
        m.merge(DelegateChunkReadStream(chunk1), cm1, bws)
        assertThatThrownBy {
          m.merge(DelegateChunkReadStream(chunk2), cm2, bws)
        }.isInstanceOf(IllegalStateException::class.java)
      }
      ctx.completeNow()
    }
  }
}
