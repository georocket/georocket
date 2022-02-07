package io.georocket.ogcapifeatures

import io.georocket.http.Endpoint
import io.georocket.ogcapifeatures.views.Views
import io.georocket.ogcapifeatures.views.json.JsonViews
import io.georocket.ogcapifeatures.views.xml.XmlViews
import io.georocket.util.atom
import io.georocket.util.core
import io.georocket.util.xmlDocument
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.Json
import io.vertx.kotlin.core.json.array
import io.vertx.kotlin.core.json.obj

/**
 * An endpoint that provides supported OGC API Features conformance classes
 * @author Michel Kraemer
 */
class ConformanceEndpoint(private val vertx: Vertx) : Endpoint {
  override suspend fun createRouter(): Router {
    val router = Router.router(vertx)
    router.get("/").produces("application/json").handler() { ctx -> onGet(ctx, JsonViews) }
    router.get("/").produces("application/xml").handler() { ctx -> onGet(ctx, XmlViews) }
    router.get("/").handler { context ->
      respondWithHttp406NotAcceptable(context, listOf("application/json", "application/xml"))
    }
    return router
  }

  private fun onGet(context: RoutingContext, views: Views) {
    val conformsTo = listOf(
      "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
      "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
      "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/gmlsf2",
      "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas3"
    )
    views.conformances(context.response(), conformsTo)
  }

}
