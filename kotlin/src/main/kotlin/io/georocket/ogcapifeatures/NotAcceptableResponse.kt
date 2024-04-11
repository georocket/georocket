package io.georocket.ogcapifeatures

import io.georocket.ogcapifeatures.views.Views
import io.vertx.core.http.HttpServerResponse
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * Responds with the 406 "Not Acceptable" status code and lists all acceptable content types in the body.
 */
fun respondWithHttp406NotAcceptable(response: HttpServerResponse, accept: List<String>) {
  response.statusCode = 406
  response.statusMessage = "Not Acceptable"
  response.putHeader("Content-Type", Views.ContentTypes.JSON)
  response.send(jsonObjectOf(
    "accept" to accept
  ).encodePrettily())
}
