package io.georocket.http

import io.vertx.ext.web.RoutingContext
import io.vertx.core.http.HttpServerResponse
import io.georocket.util.ThrowableHelper
import io.vertx.core.json.JsonObject
import io.vertx.core.eventbus.ReplyException
import io.georocket.ServerAPIException
import io.georocket.util.HttpException
import io.vertx.ext.web.Router
import java.lang.Exception

/**
 * Base interface for HTTP endpoints
 * @author Michel Kraemer
 * @since 1.2.0
 */
interface Endpoint {
  /**
   * Create a router that handles HTTP requests for this endpoint
   * @return the router
   */
  suspend fun createRouter(): Router

  suspend fun close() {
    // nothing to do by default
  }

  companion object {
    /**
     * Get absolute data store path from request
     */
    fun getEndpointPath(context: RoutingContext): String {
      val path = context.normalizedPath()
      val routePath = context.mountPoint()
      var result: String? = null
      if (routePath.length < path.length) {
        result = path.substring(routePath.length)
      }
      if (result == null || result.isEmpty()) {
        return "/"
      }
      if (result[0] != '/') {
        result = "/$result"
      }
      return result
    }

    /**
     * Let the request fail by setting the correct HTTP error code and an error
     * description in the body
     */
    fun fail(response: HttpServerResponse, throwable: Throwable) {
      response
        .setStatusCode(ThrowableHelper.throwableToCode(throwable))
        .end(throwableToJsonResponse(throwable).toString())
    }

    /**
     * Generate the JSON error response for a failed request
     */
    private fun throwableToJsonResponse(throwable: Throwable): JsonObject {
      val msg = ThrowableHelper.throwableToMessage(throwable, "")
      return try {
        JsonObject(msg)
      } catch (e: Exception) {
        if (throwable is ReplyException) {
          return ServerAPIException.toJson(ServerAPIException.GENERIC_ERROR, msg)
        }
        if (throwable is HttpException) {
          ServerAPIException.toJson(ServerAPIException.HTTP_ERROR, msg)
        } else {
          ServerAPIException.toJson(ServerAPIException.GENERIC_ERROR, msg)
        }
      }
    }
  }
}
