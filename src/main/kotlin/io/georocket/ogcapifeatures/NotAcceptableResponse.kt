package io.georocket.ogcapifeatures

import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * Responds with the 406 "Not Acceptable" status code and lists all acceptable content types in the body.
 */
fun respondWithHttp406NotAcceptable(context: RoutingContext, accept: List<String>) {
  val response = context.response()
  response.statusCode = 406
  response.statusMessage = "Not Acceptable"
  response.putHeader("Content-Type", "application/json")
  response.send(jsonObjectOf(
    "accept" to accept
  ).encodePrettily())
}
