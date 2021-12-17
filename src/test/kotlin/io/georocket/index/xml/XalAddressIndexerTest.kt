package io.georocket.index.xml

import io.georocket.coVerify
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

/**
 * Tests [XalAddressIndexer]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class XalAddressIndexerTest {
  /**
   * Indexes the given XML file and checks if the result matches the
   * expected properties map
   */
  private suspend fun assertIndexed(expected: Map<String, Any>, xmlFile: String) {
    val json = javaClass.getResource(xmlFile)!!.readText()

    val indexer = XalAddressIndexer()
    val expectedMap = mapOf("address" to expected)

    XMLTransformer().transform(Buffer.buffer(json)).collect { e ->
      indexer.onEvent(e)
    }

    Assertions.assertThat(indexer.makeResult()).isEqualTo(expectedMap)
  }

  /**
   * Test if an XML file containing a XAL address can be indexed
   */
  @Test
  fun feature(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      val expected = mapOf(
        "Country" to "Germany",
        "Locality" to "Darmstadt",
        "Street" to "Fraunhoferstra\u00DFe",
        "Number" to "5"
      )

      ctx.coVerify {
        assertIndexed(expected, "xal_simple_address.xml")
      }

      ctx.completeNow()
    }
  }
}
