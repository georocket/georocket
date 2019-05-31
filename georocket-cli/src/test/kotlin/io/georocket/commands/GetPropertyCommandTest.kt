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
 * Test for [GetPropertyCommand]
 */
@RunWith(VertxUnitRunner::class)
class GetPropertyCommandTest : CommandTestBase<GetPropertyCommand>() {
  override val cmd = GetPropertyCommand()

  /**
   * Test a simple get
   */
  @Test
  fun testGetProperty(context: TestContext) {
    val body = "a,b,c"
    val url = "/store/?property=test"
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(body)))

    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(0, exitCode)
      context.assertEquals(body, writer.toString())
      verifyRequested(url, context)
      async.complete()
    }

    cmd.run(arrayOf("-l", "/", "-prop", "test"), input, out)
  }
}
