package io.georocket.output.xml

import io.georocket.assertThatThrownBy
import io.georocket.coVerify
import io.georocket.storage.GenericXmlChunkMeta
import io.georocket.util.XMLStartElement
import io.georocket.util.io.BufferWriteStream
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
 * Test [MergeNamespacesStrategy]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class MergeNamespacesStrategyTest {
  companion object {
    private const val XMLHEADER = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>${"\n"}"""

    private val ROOT1 = XMLStartElement(null, "root",
      namespacePrefixes = listOf(null, "ns1", "xsi"),
      namespaceUris = listOf("uri0", "uri1", "http://www.w3.org/2001/XMLSchema-instance"),
      attributePrefixes = listOf("xsi", "ns1"),
      attributeLocalNames = listOf("schemaLocation", "attr1"),
      attributeValues = listOf("uri0 location0 uri1 location1", "value1")
    )
    private val ROOT2 = XMLStartElement(
      null, "root",
      namespacePrefixes = listOf(null, "ns2", "xsi"),
      namespaceUris = listOf("uri0", "uri2", "http://www.w3.org/2001/XMLSchema-instance"),
      attributePrefixes = listOf("xsi", "ns2"),
      attributeLocalNames = listOf("schemaLocation", "attr2"),
      attributeValues = listOf("uri0 location0 uri2 location2", "value2")
    )
    private val EXPECTEDROOT = XMLStartElement(
      null, "root",
      namespacePrefixes = listOf(null, "ns1", "xsi", "ns2"),
      namespaceUris = listOf("uri0", "uri1", "http://www.w3.org/2001/XMLSchema-instance", "uri2"),
      attributePrefixes = listOf("xsi", "ns1", "ns2"),
      attributeLocalNames = listOf("schemaLocation", "attr1", "attr2"),
      attributeValues = listOf("uri0 location0 uri1 location1 uri2 location2", "value1", "value2")
    )

    private const val CONTENTS1 = "<elem><ns1:child1></ns1:child1></elem>"
    private val CHUNK1 = Buffer.buffer(CONTENTS1)
    private const val CONTENTS2 = "<elem><ns2:child2></ns2:child2></elem>"
    private val CHUNK2 = Buffer.buffer(CONTENTS2)

    private val META1 = GenericXmlChunkMeta(listOf(ROOT1))
    private val META2 = GenericXmlChunkMeta(listOf(ROOT2))
  }

  /**
   * Test a simple merge
   */
  @Test
  fun simple(vertx: Vertx, ctx: VertxTestContext) {
    val strategy = MergeNamespacesStrategy()
    val bws = BufferWriteStream()

    CoroutineScope(vertx.dispatcher()).launch {
      ctx.coVerify {
        strategy.init(META1)
        strategy.init(META2)
        strategy.merge(CHUNK1, META1, bws)
        strategy.merge(CHUNK2, META2, bws)
        strategy.finish(bws)
        assertThat(bws.buffer.toString("utf-8")).isEqualTo(
            """$XMLHEADER$EXPECTEDROOT$CONTENTS1$CONTENTS2</${EXPECTEDROOT.name}>""")
      }
      ctx.completeNow()
    }
  }

  /**
   * Make sure that chunks that have not been passed to the initalize method cannot be merged
   */
  @Test
  fun mergeUninitialized(vertx: Vertx, ctx: VertxTestContext) {
    val strategy = MergeNamespacesStrategy()
    val bws = BufferWriteStream()

    CoroutineScope(vertx.dispatcher()).launch {
      ctx.coVerify {
        assertThatThrownBy {
          strategy.init(META1) // skip second init
          strategy.merge(CHUNK1, META1, bws)
          strategy.merge(CHUNK2, META2, bws)
        }.isInstanceOf(IllegalStateException::class.java)
      }
      ctx.completeNow()
    }
  }
}
