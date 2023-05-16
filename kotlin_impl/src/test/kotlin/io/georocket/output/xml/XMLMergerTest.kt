package io.georocket.output.xml

import io.georocket.coVerify
import io.georocket.storage.GenericXmlChunkMeta
import io.georocket.storage.XmlChunkMeta
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
 * Test [XMLMerger]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class XMLMergerTest {
  companion object {
    private const val XMLHEADER = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>${"\n"}"""
    private const val XSI = "xsi"
    private const val SCHEMA_LOCATION = "schemaLocation"
    private const val NS_CITYGML_1_0 = "http://www.opengis.net/citygml/1.0"
    private const val NS_CITYGML_1_0_BUILDING = "http://www.opengis.net/citygml/building/1.0"
    private const val NS_CITYGML_1_0_BUILDING_URL = "http://schemas.opengis.net/citygml/building/1.0/building.xsd"
    private const val NS_CITYGML_1_0_BUILDING_SCHEMA_LOCATION = "$NS_CITYGML_1_0_BUILDING $NS_CITYGML_1_0_BUILDING_URL"
    private const val NS_CITYGML_1_0_GENERICS = "http://www.opengis.net/citygml/generics/1.0"
    private const val NS_CITYGML_1_0_GENERICS_URL = "http://schemas.opengis.net/citygml/generics/1.0/generics.xsd"
    private const val NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION = "$NS_CITYGML_1_0_GENERICS $NS_CITYGML_1_0_GENERICS_URL"
    private const val NS_GML = "http://www.opengis.net/gml"
    private const val NS_SCHEMA_INSTANCE = "http://www.w3.org/2001/XMLSchema-instance"
  }

  private fun doMerge(
    vertx: Vertx,
    ctx: VertxTestContext,
    chunks: List<Buffer>,
    metas: List<XmlChunkMeta>,
    xmlContents: String,
    optimistic: Boolean,
    expected: Class<out Throwable>? = null
  ) {
    val m = XMLMerger(optimistic)
    val bws = BufferWriteStream()

    if (!optimistic) {
      for (meta in metas) {
        m.init(meta)
      }
    }

    CoroutineScope(vertx.dispatcher()).launch {
      ctx.coVerify {
        try {
          for ((meta, chunk) in metas.zip(chunks)) {
            m.merge(chunk, meta, bws)
          }
          m.finish(bws)
          assertThat(bws.buffer.toString("utf-8")).isEqualTo("""$XMLHEADER$xmlContents""")
          if (expected != null) {
            ctx.failNow(IllegalStateException("Excepted $expected to be thrown"))
          }
        } catch (t: Throwable) {
          if (expected == null || t::class.java != expected) {
            ctx.failNow(t)
          }
        }
      }
      ctx.completeNow()
    }
  }

  /**
   * Test if simple chunks can be merged
   */
  @Test
  fun simple(vertx: Vertx, ctx: VertxTestContext) {
    val chunk1 = Buffer.buffer("""<test chunk="1"></test>""")
    val chunk2 = Buffer.buffer("""<test chunk="2"></test>""")

    val cm = GenericXmlChunkMeta(listOf(XMLStartElement(localName = "root")))

    doMerge(
      vertx,
      ctx,
      listOf(chunk1, chunk2),
      listOf(cm, cm),
      """<root><test chunk="1"></test><test chunk="2"></test></root>""",
      false
    )
  }

  private fun mergeNamespaces(
    vertx: Vertx, ctx: VertxTestContext, optimistic: Boolean, expected: Class<out Throwable>?
  ) {
    val root1 = XMLStartElement(
      null,
      "CityModel",
      listOf(null, "gml", "gen", XSI),
      listOf(NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_GENERICS, NS_SCHEMA_INSTANCE),
      listOf(XSI),
      listOf(SCHEMA_LOCATION),
      listOf(NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION)
    )
    val root2 = XMLStartElement(
      null,
      "CityModel",
      listOf(null, "gml", "bldg", XSI),
      listOf(NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_BUILDING, NS_SCHEMA_INSTANCE),
      listOf(XSI),
      listOf(SCHEMA_LOCATION),
      listOf(NS_CITYGML_1_0_BUILDING_SCHEMA_LOCATION)
    )

    val contents1 = "<cityObjectMember><gen:GenericCityObject></gen:GenericCityObject></cityObjectMember>"
    val chunk1 = Buffer.buffer(contents1)
    val contents2 = "<cityObjectMember><bldg:Building></bldg:Building></cityObjectMember>"
    val chunk2 = Buffer.buffer(contents2)

    val cm1 = GenericXmlChunkMeta(listOf(root1))
    val cm2 = GenericXmlChunkMeta(listOf(root2))

    val expectedRoot = XMLStartElement(
      null,
      "CityModel",
      listOf(null, "gml", "gen", XSI, "bldg"),
      listOf(NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_GENERICS, NS_SCHEMA_INSTANCE, NS_CITYGML_1_0_BUILDING),
      listOf(XSI),
      listOf(SCHEMA_LOCATION),
      listOf("""$NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION $NS_CITYGML_1_0_BUILDING_SCHEMA_LOCATION""")
    )
    doMerge(
      vertx,
      ctx,
      listOf(chunk1, chunk2),
      listOf(cm1, cm2),
      """$expectedRoot$contents1$contents2</${expectedRoot.name}>""",
      optimistic,
      expected
    )
  }

  /**
   * Test if chunks with different namespaces can be merged
   */
  @Test
  fun mergeNamespaces(vertx: Vertx, ctx: VertxTestContext) {
    mergeNamespaces(vertx, ctx, false, null)
  }

  /**
   * Make sure chunks with different namespaces cannot be merged in
   * optimistic mode
   */
  @Test
  fun mergeNamespacesOptimistic(vertx: Vertx, ctx: VertxTestContext) {
    mergeNamespaces(vertx, ctx, true, IllegalStateException::class.java)
  }

  /**
   * Test if chunks with the same namespaces can be merged in optimistic mode
   */
  @Test
  fun mergeOptimistic(vertx: Vertx, ctx: VertxTestContext) {
    val root1 = XMLStartElement(
      null,
      "CityModel",
      listOf(null, "gml", "gen", XSI),
      listOf(NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_GENERICS, NS_SCHEMA_INSTANCE),
      listOf(XSI),
      listOf(SCHEMA_LOCATION),
      listOf(NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION)
    )

    val contents1 = "<cityObjectMember><gen:GenericCityObject></gen:GenericCityObject></cityObjectMember>"
    val chunk1 = Buffer.buffer(contents1)
    val contents2 = "<cityObjectMember><gen:Building></gen:Building></cityObjectMember>"
    val chunk2 = Buffer.buffer(contents2)

    val cm1 = GenericXmlChunkMeta(listOf(root1))
    val cm2 = GenericXmlChunkMeta(listOf(root1))

    doMerge(
      vertx, ctx, listOf(chunk1, chunk2), listOf(cm1, cm2), """$root1$contents1$contents2</${root1.name}>""", true
    )
  }
}
