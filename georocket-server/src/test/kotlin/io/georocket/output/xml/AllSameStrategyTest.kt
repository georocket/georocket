package io.georocket.output.xml

import io.georocket.assertThatThrownBy
import io.georocket.coVerify
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
 * Test [AllSameStrategy]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class AllSameStrategyTest {
  companion object {
    private const val XMLHEADER = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>${"\n"}"""
    private val chunk1 = Buffer.buffer("""$XMLHEADER<root><test chunk="1"></test></root>""")
    private val chunk2 = Buffer.buffer("""$XMLHEADER<root><test chunk="2"></test></root>""")
    private val cm = XMLChunkMeta(listOf(XMLStartElement("root")),
        XMLHEADER.length + 6, chunk1.length() - 7)
  }

  /**
   * Test a simple merge
   */
  @Test
  fun simple(vertx: Vertx, ctx: VertxTestContext) {
    val strategy = AllSameStrategy()
    val bws = BufferWriteStream()

    GlobalScope.launch(vertx.dispatcher()) {
      ctx.coVerify {
        strategy.init(cm)
        strategy.init(cm)
        strategy.merge(DelegateChunkReadStream(chunk1), cm, bws)
        strategy.merge(DelegateChunkReadStream(chunk2), cm, bws)
        strategy.finish(bws)

        assertThat(bws.buffer.toString("utf-8")).isEqualTo(
            """$XMLHEADER<root><test chunk="1"></test><test chunk="2"></test></root>""")
      }
      ctx.completeNow()
    }
  }

  /**
   * Test if chunks that have not been passed to the initialize method can be merged
   */
  @Test
  fun mergeUninitialized(vertx: Vertx, ctx: VertxTestContext) {
    val strategy: MergeStrategy = AllSameStrategy()
    val bws = BufferWriteStream()

    GlobalScope.launch(vertx.dispatcher()) {
      ctx.coVerify {
        strategy.init(cm) // skip second init
        strategy.merge(DelegateChunkReadStream(chunk1), cm, bws)
        strategy.merge(DelegateChunkReadStream(chunk2), cm, bws)
        strategy.finish(bws)
        assertThat(bws.buffer.toString("utf-8")).isEqualTo(
            """$XMLHEADER<root><test chunk="1"></test><test chunk="2"></test></root>""")
      }
      ctx.completeNow()
    }
  }

  /**
   * Test if canMerge works correctly
   */
  @Test
  fun canMerge(vertx: Vertx, ctx: VertxTestContext) {
    val cm2 = XMLChunkMeta(listOf(XMLStartElement("other")), 10, 20)
    val cm3 = XMLChunkMeta(listOf(XMLStartElement("pre", "root")), 10, 20)
    val cm4 = XMLChunkMeta(listOf(XMLStartElement(null, "root", arrayOf(""), arrayOf("uri"))), 10, 20)
    val strategy = AllSameStrategy()

    GlobalScope.launch(vertx.dispatcher()) {
      ctx.coVerify {
        assertThat(strategy.canMerge(cm)).isTrue
        strategy.init(cm)
        assertThat(strategy.canMerge(cm)).isTrue
        assertThat(strategy.canMerge(cm2)).isFalse
        assertThat(strategy.canMerge(cm3)).isFalse
        assertThat(strategy.canMerge(cm4)).isFalse
      }
      ctx.completeNow()
    }
  }

  /**
   * Test if the merge method fails if it is called with an unexpected chunk
   */
  @Test
  fun mergeFail(vertx: Vertx, ctx: VertxTestContext) {
    val cm2 = XMLChunkMeta(listOf(XMLStartElement("other")), 10, 20)
    val strategy = AllSameStrategy()
    val bws = BufferWriteStream()

    GlobalScope.launch(vertx.dispatcher()) {
      ctx.coVerify {
        strategy.init(cm)
        assertThatThrownBy {
          strategy.merge(DelegateChunkReadStream(chunk2), cm2, bws)
        }.isInstanceOf(IllegalStateException::class.java)
      }
      ctx.completeNow()
    }
  }
}
