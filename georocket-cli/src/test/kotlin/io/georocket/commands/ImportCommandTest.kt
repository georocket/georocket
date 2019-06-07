package io.georocket.commands

import com.github.tomakehurst.wiremock.client.VerificationException
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.verify
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import java.io.File

/**
 * Test for [ImportCommand]
 */
@RunWith(VertxUnitRunner::class)
class ImportCommandTest : CommandTestBase<ImportCommand>() {
  companion object {
    /**
     * Test XML file contents to import
     */
    private const val XML = "<test></test>"
  }

  /**
   * A temporary folder for test files
   */
  @Rule
  @JvmField
  var folder = TemporaryFolder()

  /**
   * The XML file to import
   */
  private var testFile: File? = null

  override val cmd = ImportCommand()

  @Before
  override fun setUp() {
    super.setUp()
    testFile = folder.newFile("test").apply { writeText(XML) }
  }

  /**
   * Test no file pattern
   */
  @Test
  fun noFilePattern(context: TestContext) {
    val async = context.async()
    GlobalScope.launch(rule.vertx().dispatcher()) {
      context.assertEquals(1, cmd.runAwait(arrayOf(), input, out))
      async.complete()
    }
  }

  /**
   * Verify that a certain POST request has been made
   */
  private fun verifyPosted(url: String, context: TestContext) {
    try {
      verify(postRequestedFor(urlEqualTo(url))
          .withRequestBody(equalTo(XML)))
    } catch (e: VerificationException) {
      context.fail(e)
    }
  }

  /**
   * Test a simple import
   */
  @Test
  fun simpleImport(context: TestContext) {
    val url = "/store"
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)))

    val async = context.async()
    GlobalScope.launch(rule.vertx().dispatcher()) {
      val exitCode = cmd.runAwait(arrayOf(testFile!!.absolutePath), input, out)
      context.assertEquals(0, exitCode)
      verifyPosted(url, context)
      async.complete()
    }
  }

  /**
   * Test importing to a layer
   */
  @Test
  fun importLayer(context: TestContext) {
    val url = "/store/hello/world/"
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)))

    val async = context.async()
    GlobalScope.launch(rule.vertx().dispatcher()) {
      val exitCode = cmd.runAwait(arrayOf("-l", "hello/world",
          testFile!!.absolutePath), input, out)
      context.assertEquals(0, exitCode)
      verifyPosted(url, context)
      async.complete()
    }
  }

  /**
   * Test importing with tags
   */
  @Test
  fun importTags(context: TestContext) {
    val url = "/store?tags=hello%2Cworld"
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)))

    val async = context.async()
    GlobalScope.launch(rule.vertx().dispatcher()) {
      val exitCode = cmd.runAwait(arrayOf("-t", "hello,world",
          testFile!!.absolutePath), input, out)
      context.assertEquals(0, exitCode)
      verifyPosted(url, context)
      async.complete()
    }
  }

  /**
   * Test importing with properties
   */
  @Test
  fun importProperties(context: TestContext) {
    val url = "/store?props=hello%3Aworld%2CmyKey%3AmyValue"
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)))

    val async = context.async()
    GlobalScope.launch(rule.vertx().dispatcher()) {
      val exitCode = cmd.runAwait(arrayOf("-props", "hello:world,myKey:myValue",
          testFile!!.absolutePath), input, out)
      context.assertEquals(0, exitCode)
      verifyPosted(url, context)
      async.complete()
    }
  }

  /**
   * Test importing with properties including an escaped character
   */
  @Test
  fun importPropertiesEscaping(context: TestContext) {
    val url = "/store?props=hello%3Aworld%2CmyKey%3Amy%5C%3AValue%2Cmy%5C%3AKey%3AmyValue"
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)))

    val async = context.async()
    GlobalScope.launch(rule.vertx().dispatcher()) {
      val exitCode = cmd.runAwait(arrayOf("-props",
          "hello:world,myKey:my\\:Value,my\\:Key:myValue",
          testFile!!.absolutePath), input, out)
      context.assertEquals(0, exitCode)
      verifyPosted(url, context)
      async.complete()
    }

  }

  /**
   * Test importing with tags
   */
  @Test
  fun importFallbackCRS(context: TestContext) {
    val url = "/store?fallbackCRS=test"
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)))

    val async = context.async()
    GlobalScope.launch(rule.vertx().dispatcher()) {
      val exitCode = cmd.runAwait(arrayOf("-c", "test",
          testFile!!.absolutePath), input, out)
      context.assertEquals(0, exitCode)
      verifyPosted(url, context)
      async.complete()
    }
  }
}
