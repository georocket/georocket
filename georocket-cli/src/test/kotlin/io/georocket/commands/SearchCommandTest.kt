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
 * Test for [SearchCommand]
 */
@RunWith(VertxUnitRunner::class)
class SearchCommandTest : CommandTestBase<SearchCommand>() {
  override val cmd = SearchCommand()

  /**
   * Test no query
   */
  @Test
  fun noQuery(context: TestContext) {
    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(1, exitCode)
      async.complete()
    }
    context.assertEquals(1, cmd.run(arrayOf(), input, out))
  }

  /**
   * Test empty query
   */
  @Test
  fun emptyQuery(context: TestContext) {
    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(1, exitCode)
      async.complete()
    }
    context.assertEquals(1, cmd.run(arrayOf(""), input, out))
  }

  /**
   * Test a simple query
   */
  @Test
  fun simpleQuery(context: TestContext) {
    val xml = "<test></test>"
    val url = "/store/?search=test"
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(xml)))

    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(0, exitCode)
      context.assertEquals(xml, writer.toString())
      verifyRequested(url, context)
      async.complete()
    }

    cmd.run(arrayOf("test"), input, out)
  }

  /**
   * Test a query with two terms
   */
  @Test
  fun twoTermsQuery(context: TestContext) {
    val xml = "<test></test>"
    val url = "/store/?search=test1%20test2"
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(xml)))

    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(0, exitCode)
      context.assertEquals(xml, writer.toString())
      verifyRequested(url, context)
      async.complete()
    }

    cmd.run(arrayOf("test1", "test2"), input, out)
  }

  /**
   * Test a query with a layer
   */
  @Test
  fun layer(context: TestContext) {
    val xml = "<test></test>"
    val url = "/store/hello/world/?search=test"
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(xml)))

    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(0, exitCode)
      context.assertEquals(xml, writer.toString())
      verifyRequested(url, context)
      async.complete()
    }

    cmd.run(arrayOf("-l", "hello/world", "test"), input, out)
  }

  /**
   * Test a query with a layer and optimistic merging
   */
  @Test
  fun optimisticMerging(context: TestContext) {
    val xml = "<test></test>"
    val url = "/store/hello/world/?search=test&optimisticMerging=true"
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(xml)))

    val async = context.async()
    cmd.endHandler = Handler { exitCode ->
      context.assertEquals(0, exitCode)
      context.assertEquals(xml, writer.toString())
      verifyRequested(url, context)
      async.complete()
    }

    cmd.run(arrayOf("-l", "hello/world", "--optimistic-merging", "test"), input, out)
  }
}
