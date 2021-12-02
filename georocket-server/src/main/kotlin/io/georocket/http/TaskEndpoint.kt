package io.georocket.http

import io.georocket.constants.AddressConstants
import io.georocket.http.Endpoint.Companion.fail
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.eventbus.requestAwait
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlin.coroutines.CoroutineContext

/**
 * An HTTP endpoint for accessing the information about tasks maintained by
 * the [io.georocket.tasks.TaskVerticle]
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
    onGet(context, AddressConstants.TASK_GET_ALL, null)
  }

  /**
   * Return all tasks belonging to a given correlation ID
   */
  private fun onGetByCorrelationId(context: RoutingContext) {
    val correlationId = context.pathParam("id")
    onGet(context, AddressConstants.TASK_GET_BY_CORRELATION_ID, correlationId)
  }

  /**
   * Helper method to generate a response containing task information
   */
  private fun onGet(context: RoutingContext, address: String, correlationId: String?) {
    launch {
      val task = try {
        vertx.eventBus().requestAwait<JsonObject>(address, correlationId)
      } catch (t: Throwable) {
        fail(context.response(), t)
        return@launch
      }
      context.response()
        .setStatusCode(200)
        .putHeader("Content-Type", "application/json")
        .end(task.body().encode())
    }
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
