package io.georocket.http

import io.georocket.GeoRocket
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.jsonObjectOf
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

/**
 * An HTTP endpoint for general requests
 * @author Michel Kraemer
 */
class GeneralEndpoint(private val vertx: Vertx) : Endpoint {
  private val version = getVersion()

  override suspend fun createRouter(): Router {
    val router = Router.router(vertx)
    router.get("/").handler(this::onInfo)
    router.head("/").handler(this::onPing)
    router.options("/").handler(this::onOptions)
    return router
  }

  /**
   * Send information about GeoRocket to the client
   */
  private fun onInfo(context: RoutingContext) {
    val o = jsonObjectOf(
      "name" to "GeoRocket",
      "version" to version,
      "tagline" to "It's not rocket science!"
    )
    context.response()
      .setStatusCode(200)
      .putHeader("Content-Type", "application/json")
      .end(o.encodePrettily())
  }

  /**
   * Send an empty response with a status code of 200 to the client
   */
  private fun onPing(context: RoutingContext) {
    context.response()
      .setStatusCode(200)
      .end()
  }

  /**
   * Handle the HTTP options request
   */
  private fun onOptions(context: RoutingContext) {
    context.response()
      .putHeader("Allow", "GET,HEAD")
      .setStatusCode(200)
      .end()
  }

  /**
   * @return the tool's version string
   */
  private fun getVersion(): String {
    val u = GeoRocket::class.java.getResource("version.dat")
    return IOUtils.toString(u, StandardCharsets.UTF_8)
  }
}
