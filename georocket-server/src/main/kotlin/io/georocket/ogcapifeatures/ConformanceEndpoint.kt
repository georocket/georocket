package io.georocket.ogcapifeatures

import io.georocket.http.Endpoint
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
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

    router.get("/").handler { ctx ->
      val response = ctx.response()

      response.putHeader("content-type", "application/json")

      response.end(Json.obj(
        "conformsTo" to Json.array(
          "http://www.opengis.net/spec/wfs-1/3.0/req/core",
          "http://www.opengis.net/spec/wfs-1/3.0/req/oas30",
          "http://www.opengis.net/spec/wfs-1/3.0/req/gmlsf2"
        )
      ).encodePrettily())
    }

    return router
  }
}
