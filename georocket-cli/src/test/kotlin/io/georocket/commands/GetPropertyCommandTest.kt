package io.georocket.commands

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
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
    GlobalScope.launch(rule.vertx().dispatcher()) {
      val exitCode = cmd.runAwait(arrayOf("-l", "/", "-prop", "test"), input, out)
      context.assertEquals(0, exitCode)
      context.assertEquals(body, writer.toString())
      verifyRequested(url, context)
      async.complete()
    }
  }
}
