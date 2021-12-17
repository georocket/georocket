package io.georocket.ogcapifeatures

import io.georocket.http.Endpoint
import io.georocket.util.PathUtils
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import kotlinx.coroutines.CoroutineScope
import kotlin.coroutines.CoroutineContext

/**
 * Main entry point for the OGC API Features
 * @author Michel Kraemer
 */
class OgcApiFeaturesEndpoint(override val coroutineContext: CoroutineContext,
    private val vertx: Vertx) : Endpoint, CoroutineScope {
  private val ae = ApiEndpoint(vertx)
  private val cfe = ConformanceEndpoint(vertx)
  private val cle = CollectionsEndpoint(coroutineContext, vertx)

  override suspend fun createRouter(): Router {
    val router = Router.router(vertx)
    router.get("/").handler(this::onInfo)

    router.mountSubRouter("/api", ae.createRouter())
    router.mountSubRouter("/conformance", cfe.createRouter())
    router.mountSubRouter("/collections", cle.createRouter())

    return router
  }

  override suspend fun close() {
    ae.close()
    cfe.close()
    cle.close()
  }

  private fun onInfo(context: RoutingContext) {
    val uri = context.request().path()
    val o = jsonArrayOf(
      jsonObjectOf(
        "href" to uri,
        "rel" to "self",
        "type" to "application/json",
        "title" to "GeoRocket OGC API Features main endpoint"
      ),
      jsonObjectOf(
        "href" to PathUtils.join(uri, "api"),
        "rel" to "service",
        "type" to "application/openapi+json;version=3.0",
        "title" to "API definition"
      ),
      jsonObjectOf(
        "href" to PathUtils.join(uri, "conformance"),
        "rel" to "conformance",
        "type" to "application/json",
        "title" to "Conformance classes implemented by this server"
      ),
      jsonObjectOf(
        "href" to PathUtils.join(uri, "collections"),
        "rel" to "data",
        "type" to "application/json",
        "title" to "Metadata about feature collections"
      )
    )

    context.response()
      .setStatusCode(200)
      .putHeader("Content-Type", "application/json")
      .end(o.encodePrettily())
  }
}
