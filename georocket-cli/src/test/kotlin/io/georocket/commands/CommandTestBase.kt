package io.georocket.commands

import com.github.tomakehurst.wiremock.client.VerificationException
import com.github.tomakehurst.wiremock.client.WireMock.configureFor
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import com.github.tomakehurst.wiremock.junit.WireMockRule
import de.undercouch.underline.InputReader
import de.undercouch.underline.StandardInputReader
import io.georocket.ConfigConstants
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import java.io.PrintWriter
import java.io.StringWriter

/**
 * Base class for unit tests that test commands
 */
abstract class CommandTestBase<T : AbstractGeoRocketCommand> {
  companion object {
    /**
     * Run a mock HTTP server
     */
    @ClassRule
    @JvmField
    var wireMockRule = WireMockRule(options().dynamicPort())
  }

  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  @JvmField
  var rule = RunTestOnContext()

  /**
   * The command under test
   */
  protected abstract val cmd: T

  protected val input: InputReader = StandardInputReader()
  protected val writer = StringWriter()
  protected val out = PrintWriter(writer)

  /**
   * Set up test
   */
  @Before
  open fun setUp() {
    configureFor("localhost", wireMockRule.port())

    val config = JsonObject()
        .put(ConfigConstants.HOST, "localhost")
        .put(ConfigConstants.PORT, wireMockRule.port())
    rule.vertx().orCreateContext.config().mergeIn(config)
  }

  /**
   * Verify that a certain request has been made
   */
  protected fun verifyRequested(url: String, context: TestContext) {
    try {
      verify(getRequestedFor(urlEqualTo(url)))
    } catch (e: VerificationException) {
      context.fail(e)
    }
  }
}
