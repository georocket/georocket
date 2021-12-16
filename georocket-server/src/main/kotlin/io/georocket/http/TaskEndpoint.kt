package io.georocket.http

import io.georocket.tasks.TaskRegistry
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.jsonObjectOf
import kotlinx.coroutines.CoroutineScope
import kotlin.coroutines.CoroutineContext

/**
 * An HTTP endpoint for accessing the information about tasks maintained by
 * the [io.georocket.tasks.TaskRegistry]
 * @author Michel Kraemer
 */
class TaskEndpoint(override val coroutineContext: CoroutineContext,
    private val vertx: Vertx) : Endpoint, CoroutineScope {
  override suspend fun createRouter(): Router {
    val router = Router.router(vertx)
    router.get("/").handler(this::onGetAll)
    router.get("/:id").handler(this::onGetByCorrelationId)
    router.options("/").handler(this::onOptions)
    return router
  }

  /**
   * Return all tasks
   */
  private fun onGetAll(context: RoutingContext) {
    val tasks = TaskRegistry.getAll().groupBy { it.correlationId }
      .mapValues { e -> e.value.map { JsonObject.mapFrom(it) } }
    context.response()
      .setStatusCode(200)
      .putHeader("Content-Type", "application/json")
      .end(JsonObject.mapFrom(tasks).encode())
  }

  /**
   * Return all tasks belonging to a given correlation ID
   */
  private fun onGetByCorrelationId(context: RoutingContext) {
    val correlationId = context.pathParam("id")
    val tasks = TaskRegistry.getAll().filter { it.correlationId == correlationId }
    context.response()
      .setStatusCode(200)
      .putHeader("Content-Type", "application/json")
      .end(jsonObjectOf(correlationId to tasks.map { JsonObject.mapFrom(it) }).encode())
  }

  /**
   * Handle the HTTP options request
   * @param context the context for handling HTTP requests
   */
  private fun onOptions(context: RoutingContext) {
    context.response()
      .putHeader("Allow", "GET")
      .setStatusCode(200)
      .end()
  }
}
