package io.georocket.ogcapifeatures

import io.georocket.http.Endpoint
import io.georocket.ogcapifeatures.views.Views
import io.georocket.ogcapifeatures.views.json.JsonViews
import io.georocket.ogcapifeatures.views.xml.XmlViews
import io.georocket.util.PathUtils
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import kotlinx.coroutines.CoroutineScope
import kotlin.coroutines.CoroutineContext

/**
 * Main entry point for the OGC API Features
 * @author Michel Kraemer
 */
class LandingPageEndpoint(
  override val coroutineContext: CoroutineContext, private val vertx: Vertx
) : Endpoint, CoroutineScope {
  private val ae = ApiEndpoint(vertx)
  private val cfe = ConformanceEndpoint(vertx)
  private val cle = CollectionsEndpoint(coroutineContext, vertx)

  override suspend fun createRouter(): Router {
    val router = Router.router(vertx)
    router.get("/").produces("application/json").handler { ctx -> onInfo(ctx, JsonViews) }
    router.get("/").produces("application/xml").handler { ctx -> onInfo(ctx, XmlViews) }
    router.get("/").handler { context ->
      respondWithHttp406NotAcceptable(context, listOf("application/json", "application/xml"))
    }

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

  private fun onInfo(context: RoutingContext, views: Views) {
    val basePath = context.request().path()
    val links = Endpoint.getLinksToSelf(context) + listOf(
      Views.Link(
        href = PathUtils.join(basePath, "conformance"),
        rel = "conformance",
        type = "application/json",
        title = "Conformance classes implemented by this server as JSON"
      ),
      Views.Link(
        href = PathUtils.join(basePath, "conformance"),
        rel = "conformance",
        type = "application/xml",
        title = "Conformance classes implemented by this server as XML"
      ),
      Views.Link(
        href = PathUtils.join(basePath, "collections"),
        rel = "data",
        type = "application/json",
        title = "Metadata about feature collections as JSON"
      ),
      Views.Link(
        href = PathUtils.join(basePath, "collections"),
        rel = "data",
        type = "application/xml",
        title = "Metadata about feature collections as XML"
      ),
      Views.Link(
        href = PathUtils.join(basePath, "api"),
        rel = "service-desc",
        type = "application/vnd.oai.openapi+json;version=3.0",
        title = "API definition"
      ),
    )
    views.landingPage(context.response(), links)
  }

}
