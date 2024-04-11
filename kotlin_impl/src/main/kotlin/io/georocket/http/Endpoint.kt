package io.georocket.http

import io.georocket.ServerAPIException
import io.georocket.ogcapifeatures.views.Views
import io.georocket.util.HttpException
import io.georocket.util.ThrowableHelper
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext

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

    fun getLinksToSelf(context: RoutingContext, supportsJson: Boolean = true, supportsXml: Boolean = true, ): List<Views.Link> {
      val path = context.request().path()
      val links = mutableListOf<Views.Link>()
      if (supportsJson) {
        links.add(Views.Link(
          href = path,
          type = "application/json",
          rel = when (context.acceptableContentType) {
            "application/json" -> "self"
            else -> "alternate"
          },
          title = when (context.acceptableContentType) {
            "application/json" -> "This document"
            else -> "This document in JSON"
          },
        ))
      }
      if (supportsXml) {
        links.add(Views.Link(
          href = path,
          type = "application/xml",
          rel = when (context.acceptableContentType) {
            "application/xml" -> "self"
            else -> "alternate"
          },
          title = when (context.acceptableContentType) {
            "application/xml" -> "This document"
            else -> "This document in XML"
          },
        ))
      }
      return links
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
