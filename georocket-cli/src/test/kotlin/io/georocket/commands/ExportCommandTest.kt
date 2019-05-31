package io.georocket.commands

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import io.vertx.core.Handler
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Test for [ExportCommand]
 */
@RunWith(VertxUnitRunner::class)
class ExportCommandTest : CommandTestBase<ExportCommand>() {
  override val cmd = ExportCommand()

  /**
   * Test no layer
   */
  @Test
  fun noLayer(context: TestContext) {
    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(1, exitCode)
      async.complete()
    }
    cmd.run(arrayOf(), input, out)
  }

  /**
   * Test empty layer
   */
  @Test
  fun emptyLayer(context: TestContext) {
    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(1, exitCode)
      async.complete()
    }
    cmd.run(arrayOf(""), input, out)
  }

  /**
   * Test if the root layer can be exported
   */
  @Test
  fun exportRoot(context: TestContext) {
    val xml = "<test></test>"
    val url = "/store/"
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(xml)))

    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(0, exitCode)
      verifyRequested(url, context)
      async.complete()
    }

    cmd.run(arrayOf("/"), input, out)
  }

  /**
   * Test if a layer can be exported
   */
  @Test
  fun exportLayer(context: TestContext) {
    val xml = "<test></test>"
    val url = "/store/hello/world/"
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(xml)))

    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(0, exitCode)
      verifyRequested(url, context)
      async.complete()
    }

    cmd.run(arrayOf("/hello/world"), input, out)
  }

  /**
   * Test if a layer can be exported with optimistic merging
   */
  @Test
  fun optimisticMerging(context: TestContext) {
    val xml = "<test></test>"
    val url = "/store/hello/world/?optimisticMerging=true"
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(xml)))

    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(0, exitCode)
      verifyRequested(url, context)
      async.complete()
    }

    cmd.run(arrayOf("--optimistic-merging", "/hello/world"), input, out)
  }
}
